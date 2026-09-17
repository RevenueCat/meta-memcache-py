import os
import pickle
import sqlite3
import threading
import time
import weakref
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from meta_memcache.extras.probabilistic_hot_cache import ProbabilisticHotCache
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.metrics.base import BaseMetricsCollector, MetricDefinition
from meta_memcache.protocol import Key

# No row id, so the table is layed out following the PK
# and searching values by key is a single index lookup.
# No index in expiration, as it will penalize writes and
# changes too often, on every revalidation. Scanning the
# whole table is acceptable for the occasional purge,
# the table is bounded in size and deletes are likely
# more costly than the actual scan.
#
# `expiration` is when the value goes stale and is never mutated after the
# store, so it is also the hard deadline: the entry must not be served past
# expiration + max_stale_while_revalidate_seconds. `revalidate_at` is the
# separate retry clock the workers race on to elect a single revalidator.
_TABLE_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS hot_cache ("
    "key TEXT PRIMARY KEY, "
    "value BLOB NOT NULL, "
    "expiration INTEGER NOT NULL, "
    "revalidate_at INTEGER NOT NULL"
    ") WITHOUT ROWID"
)
_GET = "SELECT value, expiration, revalidate_at FROM hot_cache WHERE key = ?"
# The winner of the revalidation race atomically pushes the retry clock
# forward, so every other worker keeps serving the stale value while the
# winner refreshes it. The guard is a compare-and-swap: only the worker that
# matches the revalidate_at it read gets to be the winner.
_WIN_REVALIDATION = (
    "UPDATE hot_cache SET revalidate_at = ? WHERE key = ? AND revalidate_at = ?"
)
_STORE = (
    "INSERT OR REPLACE INTO hot_cache (key, value, expiration, revalidate_at) "
    "VALUES (?, ?, ?, ?)"
)
_CLEAR = "DELETE FROM hot_cache WHERE key = ? AND expiration <= ?"
_PURGE_EXPIRED = "DELETE FROM hot_cache WHERE expiration <= ?"
_COUNT = "SELECT COUNT(*) FROM hot_cache"

DEFAULT_MAX_SIZE_BYTES = 64 * 1024 * 1024
# The write-ahead log lives next to the db file and is not covered by
# max_page_count, so it is memory spent on top of max_size_bytes.
DEFAULT_MAX_WAL_BYTES = 8 * 1024 * 1024
_BUSY_TIMEOUT_MS = 100

# Hitting the max_page_count cap is reported as SQLITE_FULL, and it is the
# only failure a purge can fix. sqlite3.Error.sqlite_errorcode is only
# available from python 3.11: without it we cannot tell a full db from a
# busy one, so we assume it is full, which is the best we can do.
_SQLITE_FULL: int = getattr(sqlite3, "SQLITE_FULL", 13)


def _is_db_full(error: sqlite3.Error) -> bool:
    return getattr(error, "sqlite_errorcode", _SQLITE_FULL) == _SQLITE_FULL


def _get_instance_registry() -> "weakref.WeakSet[SqliteProbabilisticHotCache]":
    """Get the instance registry, registering the at-fork handler on first call.

    Sqlite connections must not be used by a child process after fork(),
    so the handler drops the thread-local connections of all live instances
    in the child, and they get lazily re-created. Follows the same pattern
    as meta_memcache.connection.pool.
    """
    global _instance_registry
    if _instance_registry is None:
        _instance_registry = weakref.WeakSet()
        if hasattr(os, "register_at_fork"):
            registry = _instance_registry
            os.register_at_fork(
                after_in_child=lambda: _reset_instances_after_fork(registry)
            )
    return _instance_registry


_instance_registry: Optional["weakref.WeakSet[SqliteProbabilisticHotCache]"] = None


def _reset_instances_after_fork(
    registry: "weakref.WeakSet[SqliteProbabilisticHotCache]",
) -> None:
    for instance in list(registry):
        instance._local = threading.local()


@dataclass(frozen=True)
class HotCacheDBConfig:
    """
    Reference to an initialized hot cache database.

    Encapsulates the db file location and size cap, and validates on
    construction that the database exists and is initialized. Create the
    database with HotCacheDBConfig.initialize() on server startup.
    """

    db_path: str
    max_size_bytes: int = DEFAULT_MAX_SIZE_BYTES
    max_wal_bytes: int = DEFAULT_MAX_WAL_BYTES

    @classmethod
    def initialize(
        cls,
        db_path: str,
        max_size_bytes: int = DEFAULT_MAX_SIZE_BYTES,
        recreate: bool = False,
        max_wal_bytes: int = DEFAULT_MAX_WAL_BYTES,
    ) -> "HotCacheDBConfig":
        """
        Create the hot cache database if it does not exist already.

        Hook this into your server startup (eg: gunicorn's on_starting) so a
        single worker creates the database before the rest are forked. It is
        idempotent and protected by sqlite's locking, so concurrent calls are
        also safe, just unnecessary.

        With recreate=True any pre-existing database is deleted first, useful
        to start fresh on each deploy.

        Returns the HotCacheDBConfig to build SqliteProbabilisticHotCache
        with. Workers that didn't run the initialization construct it
        themselves, which validates the database is ready.
        """
        if recreate:
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.unlink(db_path + suffix)
                except FileNotFoundError:
                    pass
        conn = sqlite3.connect(db_path)
        try:
            # WAL is a persistent, database-level setting: readers and writers
            # don't block each other, which is what makes sharing the cache
            # across workers fast.
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute(_TABLE_SCHEMA)
            conn.commit()
        finally:
            conn.close()
        return cls(
            db_path=db_path,
            max_size_bytes=max_size_bytes,
            max_wal_bytes=max_wal_bytes,
        )

    def __post_init__(self) -> None:
        if not os.path.exists(self.db_path):
            raise ValueError(
                f"Hot cache db {self.db_path} does not exist. Create it "
                "with HotCacheDBConfig.initialize() on server startup"
            )
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(_GET, ("",)).fetchone()
        except sqlite3.Error as e:
            raise ValueError(
                f"Hot cache db {self.db_path} is not initialized: {e}"
            ) from e
        finally:
            conn.close()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, isolation_level=None)  # autocommit
        page_size: int = conn.execute("PRAGMA page_size").fetchone()[0]
        # Cap the db file size. This pragma is per-connection.
        max_page_count = max(self.max_size_bytes // page_size, 16)
        conn.execute(f"PRAGMA max_page_count = {max_page_count}")
        # Bound the write-ahead log: checkpoint it once it reaches the
        # limit, and truncate it back down whenever a checkpoint resets it.
        # Without this it grows to its high-water mark and stays there.
        conn.execute(f"PRAGMA journal_size_limit = {self.max_wal_bytes}")
        conn.execute(
            f"PRAGMA wal_autocheckpoint = {max(self.max_wal_bytes // page_size, 1)}"
        )
        # It is just a cache: skip fsyncs, we don't need durability
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        # Memory-map the whole db: reads become pointer accesses into the OS
        # page cache, which is shared across all workers.
        conn.execute(f"PRAGMA mmap_size = {self.max_size_bytes}")
        return conn


class SqliteProbabilisticHotCache(ProbabilisticHotCache):
    """
    ProbabilisticHotCache backed by a sqlite file shared across workers.

    Instead of each worker holding its own copy of the hot values, they
    all share a single memory-mapped sqlite database.

    This has a number of benefits:
    * saving memory (no duplicate hot values across workers)
    * hotness detection is shared across workers, workers will
      be faster at warming up the cache.
    * less load for revalidations (only one worker revalidates)
    * the hot keys can be persisted across server restarts,
      reducing the cold start load.

    Place the db file in a memory-backed filesystem
    (eg: /dev/shm/hot_cache.db) for maximum performance.

    The database is described by a HotCacheDBConfig: create it on server
    startup with HotCacheDBConfig.initialize().

    Only one worker (across all processes and threads) gets to revalidate
    a stale value: it atomically pushes the entry's retry clock forward, so
    the others serve the stale value while it refreshes. If the winner
    fails to refresh, another worker is elected to retry
    revalidation_retry_seconds later.

    Stale values are only served within a bounded grace window: an entry is
    dropped once it is more than max_stale_while_revalidate_seconds past its
    expiration, regardless of how often it is read. If the server keeps
    failing to revalidate it, the entry expires rather than being served
    stale forever, and the key has to be detected as hot again.

    The db file is capped at db.max_size_bytes. When full, expired entries
    are purged; if there is still no room the store is skipped: the hot
    cache is always best effort and never fails the request. Entries past
    their hard deadline are also purged every purge_interval_seconds, so
    the db does not have to fill up before they get evicted.

    Budget db.max_size_bytes + db.max_wal_bytes of memory for it: the
    write-ahead log sits next to the db file and is not covered by the db
    size cap. Each purge also truncates the log, when it can do so without
    waiting on the other workers.

    All values are pickled, since they must be shared across processes.
    """

    _METRICS = ProbabilisticHotCache._METRICS + (
        MetricDefinition(
            "errors",
            "Sqlite or unpickling failures. They are ignored, since the hot "
            "cache is best effort: a failing lookup behaves as a miss, and a "
            "value that cannot be stored is simply not cached",
        ),
    )

    def __init__(
        self,
        client: CacheApi,
        db: HotCacheDBConfig,
        cache_ttl: int,
        max_last_access_age_seconds: int,
        probability_factor: int,
        max_stale_while_revalidate_seconds: int = 10,
        allowed_prefixes: Optional[List[str]] = None,
        metrics_collector: Optional[BaseMetricsCollector] = None,
        purge_interval_seconds: int = 60,
        revalidation_retry_seconds: int = 1,
    ) -> None:
        super().__init__(
            client=client,
            store={},  # Unused: storage is overridden to use sqlite
            cache_ttl=cache_ttl,
            max_last_access_age_seconds=max_last_access_age_seconds,
            probability_factor=probability_factor,
            max_stale_while_revalidate_seconds=max_stale_while_revalidate_seconds,
            allowed_prefixes=allowed_prefixes,
            metrics_collector=metrics_collector,
            revalidation_retry_seconds=revalidation_retry_seconds,
        )
        self._db = db
        self._purge_interval_seconds = purge_interval_seconds
        self._local = threading.local()
        _get_instance_registry().add(self)

    def _get_conn(self) -> sqlite3.Connection:
        # One connection per thread, dropped in children after fork (see
        # _get_instance_registry): sqlite connections must not be shared
        # across threads or processes.
        conn: Optional[sqlite3.Connection] = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = self._db.connect()
            self._local.next_purge_at = time.time() + self._purge_interval_seconds
        return conn

    def _lookup_hot_cache(
        self,
        key: Key,
    ) -> Tuple[bool, bool, Optional[Any]]:
        is_found = False
        is_hot = False
        value: Optional[Any] = None
        try:
            conn = self._get_conn()
            row = conn.execute(_GET, (key.key,)).fetchone()
            if row is not None:
                blob, expiration, revalidate_at = row
                now = int(time.time())
                is_hot = True
                if now < expiration:
                    is_found = True
                elif now < expiration + self._max_stale_while_revalidate_seconds:
                    # Expired, but within the grace window: use
                    # stale-while-revalidate to avoid thundering herds. Only
                    # one worker wins the atomic update pushing the retry
                    # clock forward, and gets to refresh the cache by
                    # mimicking a cache miss. The rest serve the stale value.
                    # If the winner dies without refreshing, another one is
                    # elected once the retry clock arrives, until the grace
                    # window runs out.
                    won = now >= revalidate_at and (
                        conn.execute(
                            _WIN_REVALIDATION,
                            (
                                now + self._revalidation_retry_seconds,
                                key.key,
                                revalidate_at,
                            ),
                        ).rowcount
                        > 0
                    )
                    is_found = not won
                else:
                    # Past the grace window: nobody managed to revalidate it,
                    # so the value is too stale to serve. Drop it and treat
                    # the key as cold, it has to be detected as hot again.
                    self._clear_hot_cache_if_necessary(key)
                    is_hot = False
                if is_found:
                    value = pickle.loads(blob)
        except (sqlite3.Error, pickle.PickleError):
            # Best effort: a failing hot cache behaves as a miss.
            is_found = False
            is_hot = False
            value = None
            self._metrics and self._metrics.metric_inc("errors")

        self._metrics and self._metrics.metric_inc("hits" if is_found else "misses")
        return is_found, is_hot, value

    def _store_entry(self, key: Key, value: Any) -> None:
        blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        # A fresh value is revalidated as soon as it goes stale.
        expiration = int(time.time()) + self._cache_ttl
        try:
            conn = self._get_conn()
            try:
                conn.execute(_STORE, (key.key, blob, expiration, expiration))
            except sqlite3.OperationalError as e:
                if not _is_db_full(e):
                    # A busy or otherwise broken db: not something a purge
                    # would fix, so don't pay for one.
                    raise
                # The db hit its size cap: purge the entries past their hard
                # deadline and retry once. If there is still no room, the
                # value is simply not cached.
                self._purge_expired(conn)
                conn.execute(_STORE, (key.key, blob, expiration, expiration))
            else:
                # Purge on a fixed schedule so the db doesn't have to fill up
                # before entries get evicted. On a timer rather than at
                # random, so the cost (a full table scan for the gauge, plus
                # a checkpoint) stays the same whatever the write rate is.
                if time.time() >= self._local.next_purge_at:
                    self._local.next_purge_at = (
                        time.time() + self._purge_interval_seconds
                    )
                    self._purge_expired(conn)
        except sqlite3.Error:
            # Best effort: the value is simply not cached.
            self._metrics and self._metrics.metric_inc("errors")

    def _purge_expired(self, conn: sqlite3.Connection) -> None:
        # Delete the entries past their hard deadline. Those still within
        # the stale-while-revalidate grace window are servable, so they stay.
        bound = int(time.time()) - self._max_stale_while_revalidate_seconds
        conn.execute(_PURGE_EXPIRED, (bound,))
        self._checkpoint(conn)
        if self._metrics:
            self._metrics.gauge_set("item_count", conn.execute(_COUNT).fetchone()[0])

    def _checkpoint(self, conn: sqlite3.Connection) -> None:
        # Hand the pages the purge just freed back to the db file and
        # truncate the log. TRUNCATE has to wait for the readers to finish,
        # so drop the busy timeout first: if another worker is mid-read we
        # skip it and the next purge tries again, rather than stalling the
        # request that happened to trigger this one.
        conn.execute("PRAGMA busy_timeout = 0")
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        finally:
            conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")

    def _clear_hot_cache_if_necessary(self, key: Key) -> bool:
        # Called when the server missed, and when an entry runs out of grace
        # window: drop the stale entry. Since expiration is never mutated
        # after the store, the guard is exact: a fresh value stored by
        # another worker in the meantime is preserved.
        now = int(time.time())
        try:
            conn = self._get_conn()
            return conn.execute(_CLEAR, (key.key, now)).rowcount > 0
        except sqlite3.Error:
            self._metrics and self._metrics.metric_inc("errors")
            return False  # Best effort
