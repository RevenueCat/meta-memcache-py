import os
import pickle
import random
import sqlite3
import threading
import time
import weakref
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from meta_memcache.extras.probabilistic_hot_cache import ProbabilisticHotCache
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.metrics.base import BaseMetricsCollector
from meta_memcache.protocol import Key

# No row id, so the table is layed out following the PK
# and searching values by key is a single index lookup.
# No index in expiration, as it will penalize writes and
# changes too often, on every revalidation. Scanning the
# whole table is acceptable for the occasional purge,
# the table is bounded in size and deletes are likely
# more costly than the actual scan.
_TABLE_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS hot_cache ("
    "key TEXT PRIMARY KEY, "
    "value BLOB NOT NULL, "
    "expiration INTEGER NOT NULL"
    ") WITHOUT ROWID"
)
_GET = "SELECT value, expiration FROM hot_cache WHERE key = ?"
# The winner of the revalidation race atomically sets the next check time,
# so every other worker keeps serving the stale value while the winner
# refreshes it. The expiration guard is a compare-and-swap: only the worker
# that matches the expiration it read gets to be the winner.
_WIN_REVALIDATION = (
    "UPDATE hot_cache SET expiration = ? WHERE key = ? AND expiration = ?"
)
_STORE = "INSERT OR REPLACE INTO hot_cache (key, value, expiration) VALUES (?, ?, ?)"
_CLEAR = "DELETE FROM hot_cache WHERE key = ? AND expiration <= ?"
_PURGE_EXPIRED = "DELETE FROM hot_cache WHERE expiration <= ?"
_COUNT = "SELECT COUNT(*) FROM hot_cache"

DEFAULT_MAX_SIZE_BYTES = 64 * 1024 * 1024


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

    @classmethod
    def initialize(
        cls,
        db_path: str,
        max_size_bytes: int = DEFAULT_MAX_SIZE_BYTES,
        recreate: bool = False,
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
        return cls(db_path=db_path, max_size_bytes=max_size_bytes)

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
        # It is just a cache: skip fsyncs, we don't need durability
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA busy_timeout = 100")
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
    an expired value: it atomically sets the entry's next check time, so
    the others serve the stale value while it refreshes. If the winner
    fails to refresh, a new worker is elected to retry after
    max_stale_while_revalidate_seconds: hot values keep being served and
    revalidated for as long as they are read, and are only dropped when
    deleted from the server, or purged once they stop being read.

    The db file is capped at db.max_size_bytes. When full, expired entries
    are purged; if there is still no room the store is skipped: the hot
    cache is always best effort and never fails the request.

    All values are pickled, since they must be shared across processes.
    """

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
        purge_probability_factor: int = 64,
    ) -> None:
        if cache_ttl <= max_stale_while_revalidate_seconds:
            # _clear_hot_cache_if_necessary() relies on this to tell fresh
            # values apart from entries under revalidation.
            raise ValueError(
                "cache_ttl must be greater than max_stale_while_revalidate_seconds"
            )
        super().__init__(
            client=client,
            store={},  # Unused: storage is overridden to use sqlite
            cache_ttl=cache_ttl,
            max_last_access_age_seconds=max_last_access_age_seconds,
            probability_factor=probability_factor,
            max_stale_while_revalidate_seconds=max_stale_while_revalidate_seconds,
            allowed_prefixes=allowed_prefixes,
            metrics_collector=metrics_collector,
        )
        self._db = db
        self._purge_probability_factor = purge_probability_factor
        self._local = threading.local()
        _get_instance_registry().add(self)

    def _get_conn(self) -> sqlite3.Connection:
        # One connection per thread, dropped in children after fork (see
        # _get_instance_registry): sqlite connections must not be shared
        # across threads or processes.
        conn: Optional[sqlite3.Connection] = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = self._db.connect()
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
                blob, expiration = row
                now = int(time.time())
                is_hot = True
                if expiration > now:
                    is_found = True
                else:
                    # Expired: use stale-while-revalidate to avoid thundering
                    # herds. Only one worker wins the atomic update setting
                    # the next check time, and gets to refresh the cache by
                    # mimicking a cache miss. The rest serve the stale value.
                    # If the winner dies without refreshing, a new winner is
                    # elected to retry once the next check time arrives.
                    won = (
                        conn.execute(
                            _WIN_REVALIDATION,
                            (
                                now + self._max_stale_while_revalidate_seconds,
                                key.key,
                                expiration,
                            ),
                        ).rowcount
                        > 0
                    )
                    is_found = not won
                if is_found:
                    value = pickle.loads(blob)
        except (sqlite3.Error, pickle.PickleError):
            # Best effort: a failing hot cache behaves as a miss.
            is_found = False
            is_hot = False
            value = None

        self._metrics and self._metrics.metric_inc("hits" if is_found else "misses")
        return is_found, is_hot, value

    def _store_entry(self, key: Key, value: Any) -> None:
        blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        expiration = int(time.time()) + self._cache_ttl
        try:
            conn = self._get_conn()
            try:
                conn.execute(_STORE, (key.key, blob, expiration))
            except sqlite3.OperationalError:
                # Likely "database or disk is full": purge expired entries
                # and retry once. If there is still no room, the value is
                # simply not cached.
                self._purge_expired(conn)
                conn.execute(_STORE, (key.key, blob, expiration))
            else:
                # Occasionally purge expired entries so the db doesn't have
                # to fill up before they get evicted.
                if random.getrandbits(10) % self._purge_probability_factor == 0:
                    self._purge_expired(conn)
        except sqlite3.Error:
            pass  # Best effort

    def _purge_expired(self, conn: sqlite3.Connection) -> None:
        # Keep entries within the stale-while-revalidate window, they
        # are still servable.
        expiration = int(time.time()) - self._max_stale_while_revalidate_seconds
        conn.execute(_PURGE_EXPIRED, (expiration,))
        if self._metrics:
            self._metrics.gauge_set("item_count", conn.execute(_COUNT).fetchone()[0])

    def _clear_hot_cache_if_necessary(self, key: Key) -> bool:
        # Called when the server missed: the key no longer exists, so drop
        # the stale entry, including one we hold the revalidation of, which
        # has expiration <= now + max_stale_while_revalidate_seconds. Fresh
        # values (expiration = now + cache_ttl) stored by another worker in
        # the meantime are preserved.
        bound = int(time.time()) + self._max_stale_while_revalidate_seconds
        try:
            conn = self._get_conn()
            return conn.execute(_CLEAR, (key.key, bound)).rowcount > 0
        except sqlite3.Error:
            return False  # Best effort
