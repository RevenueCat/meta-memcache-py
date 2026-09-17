"""Tests specific to the sqlite-backed ProbabilisticHotCache.

The behaviour it shares with the in-memory implementation is covered in
probabilistic_hot_cache_conformance_test.py. What is left here is what only
the sqlite store does: the database lifecycle, sharing the hot values (and
the revalidation election) across workers, the size cap and the purge, and
surviving a fork.
"""

import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional
from unittest.mock import Mock, call

import pytest

from meta_memcache import Key
from meta_memcache.errors import MemcacheError
from meta_memcache.extras.probabilistic_hot_cache_sqlite import (
    HotCacheDBConfig,
    SqliteProbabilisticHotCache,
)
from meta_memcache.metrics.base import BaseMetricsCollector
from tests.hot_cache_harness import (
    hot,
    revalidating,
    DEFAULT_SETTINGS,
    make_client,
)


@pytest.fixture
def client() -> Mock:
    return make_client()


@pytest.fixture
def time(monkeypatch) -> Mock:
    time_mock = Mock()
    time_mock.time.return_value = 0
    monkeypatch.setattr(
        "meta_memcache.extras.probabilistic_hot_cache_sqlite.time", time_mock
    )
    return time_mock


@pytest.fixture
def db(tmp_path: Path) -> HotCacheDBConfig:
    return HotCacheDBConfig.initialize(str(tmp_path / "hot.db"))


def build_cache(
    client: Mock, db: HotCacheDBConfig, **overrides
) -> SqliteProbabilisticHotCache:
    return SqliteProbabilisticHotCache(
        client=client,
        db=db,
        # Purging is on a timer: park it unless the test is about the
        # purge itself.
        **{**DEFAULT_SETTINGS, "purge_interval_seconds": 1 << 30, **overrides},
    )


def row_count(db: HotCacheDBConfig) -> int:
    conn = sqlite3.connect(db.db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM hot_cache").fetchone()[0]
    finally:
        conn.close()


def test_initialize_creates_and_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "hot.db"
    db = HotCacheDBConfig.initialize(str(db_path), max_size_bytes=1024 * 1024)
    assert db == HotCacheDBConfig(str(db_path), max_size_bytes=1024 * 1024)
    assert db_path.exists()
    assert row_count(db) == 0
    HotCacheDBConfig.initialize(str(db_path))  # Idempotent


def test_initialize_recreate_starts_fresh(tmp_path: Path) -> None:
    db = HotCacheDBConfig.initialize(str(tmp_path / "hot.db"))
    conn = sqlite3.connect(db.db_path)
    conn.execute(
        "INSERT INTO hot_cache (key, value, expiration, revalidate_at) "
        "VALUES ('k', x'00', 100, 100)"
    )
    conn.commit()
    conn.close()
    assert row_count(db) == 1

    db = HotCacheDBConfig.initialize(db.db_path, recreate=True)
    assert row_count(db) == 0


def test_config_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        HotCacheDBConfig(str(tmp_path / "missing.db"))


def test_config_requires_initialized_db(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.db"
    db_path.touch()
    with pytest.raises(ValueError, match="not initialized"):
        HotCacheDBConfig(str(db_path))


def test_config_rejects_an_outdated_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE hot_cache (key TEXT PRIMARY KEY, value BLOB)")
    conn.commit()
    conn.close()
    with pytest.raises(ValueError, match="not initialized"):
        HotCacheDBConfig(str(db_path))


def test_config_validates_initialized_db(db: HotCacheDBConfig) -> None:
    # Workers that didn't run initialize() build the config themselves
    assert HotCacheDBConfig(db.db_path) == HotCacheDBConfig(db.db_path)


def test_cache_is_shared_across_workers(time: Mock, db: HotCacheDBConfig) -> None:
    client_a, client_b = make_client(), make_client()
    worker_a = build_cache(client_a, db)
    worker_b = build_cache(client_b, HotCacheDBConfig(db.db_path))

    assert worker_a.get("foo_hot") == 1
    client_a.meta_get.assert_called_once()

    # Worker B hits the shared hot cache without touching the server
    assert worker_b.get("foo_hot") == 1
    client_b.meta_get.assert_not_called()


def test_single_worker_wins_revalidation(time: Mock, db: HotCacheDBConfig) -> None:
    worker_a = build_cache(make_client(), db)
    worker_b = build_cache(make_client(), db)

    assert worker_a.get("foo_hot") == 1  # Stored, expires at 60

    time.time.return_value = 61  # Stale, within the grace window
    # The first worker wins the election: it sees a miss so it will refresh
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == revalidating(1)
    # Every other worker is served the stale value while it does, including
    # other lookups from the winner's own process
    assert worker_b._lookup_hot_cache(Key("foo_hot")) == hot(1)
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == hot(1)


def test_winner_refreshes_the_value_for_everybody(
    time: Mock, db: HotCacheDBConfig
) -> None:
    client_a, client_b = make_client(), make_client()
    worker_a = build_cache(client_a, db)
    worker_b = build_cache(client_b, db)

    assert worker_a.get("foo_hot") == 1
    client_a.meta_get.reset_mock()

    time.time.return_value = 61  # Stale, within the grace window
    assert worker_a.get("foo_hot") == 1  # Wins and refreshes from the server
    client_a.meta_get.assert_called_once()

    # The refreshed value is fresh again for everybody: at t=75 the original
    # entry would be past its hard deadline (60 + 10), so a hit without a
    # new revalidation proves the refresh stored a fresh value
    time.time.return_value = 75
    assert worker_b.get("foo_hot") == 1
    client_b.meta_get.assert_not_called()


def test_revalidation_is_retried_when_a_worker_dies(
    time: Mock, db: HotCacheDBConfig
) -> None:
    worker_a = build_cache(make_client(), db, revalidation_retry_seconds=3)
    worker_b = build_cache(make_client(), db, revalidation_retry_seconds=3)

    assert worker_a.get("foo_hot") == 1

    time.time.return_value = 61
    # Worker A wins the election (retry clock set to 64)... and dies
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == revalidating(1)
    # Meanwhile everybody else serves the stale value
    assert worker_b._lookup_hot_cache(Key("foo_hot")) == hot(1)

    time.time.return_value = 64  # Retry clock reached: a new worker retries
    assert worker_b._lookup_hot_cache(Key("foo_hot")) == revalidating(1)
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == hot(1)


def test_purge_keeps_the_entries_still_servable(
    client: Mock, time: Mock, db: HotCacheDBConfig
) -> None:
    cache = build_cache(client, db, purge_interval_seconds=0)

    assert cache.get("foo_hot") == 1  # Expires at 60, dropped at 70

    # A store within the grace window purges, but the stale entry is still
    # servable so it stays
    time.time.return_value = 69
    assert cache.get("bar_hot") == 1
    assert row_count(db) == 2

    # Past the hard deadline it is purged
    time.time.return_value = 70
    assert cache.get("baz_hot") == 1
    assert cache._lookup_hot_cache(Key("foo_hot")) is None


def test_max_size_is_enforced_best_effort(time: Mock, tmp_path: Path) -> None:
    db = HotCacheDBConfig.initialize(str(tmp_path / "hot.db"), max_size_bytes=64 * 1024)
    cache = build_cache(make_client(), db)

    blob = b"x" * 4096
    for i in range(50):
        cache._store_entry(Key(f"key_{i}"), blob)  # Never raises when full

    stored = row_count(db)
    assert 0 < stored < 50  # Capped: some stores were skipped
    assert (tmp_path / "hot.db").stat().st_size <= 64 * 1024

    # Once entries are past their hard deadline, storing purges them and
    # succeeds again
    time.time.return_value = 100  # Beyond expiration (60) + grace window (10)
    cache._store_entry(Key("fresh"), blob)
    assert cache._lookup_hot_cache(Key("fresh")) == hot(blob)
    assert row_count(db) < stored


@pytest.mark.skipif(
    not hasattr(os, "fork"), reason="fork not available on this platform"
)
def test_connections_are_reset_after_fork(time: Mock, db: HotCacheDBConfig) -> None:
    cache = build_cache(make_client(), db)
    assert cache.get("foo_hot") == 1  # Stored in the shared db
    parent_conn = cache._get_conn()
    assert cache._get_conn() is parent_conn  # Cached per thread

    r_fd, w_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # Child
        os.close(r_fd)
        try:
            # The at-fork handler dropped the parent's connection
            assert getattr(cache._local, "conn", None) is None
            # And the cache still works, lazily reconnecting
            assert cache._lookup_hot_cache(Key("foo_hot")) == hot(1)
            os.write(w_fd, b"OK")
        except BaseException as e:  # noqa: BLE001
            os.write(w_fd, f"ERROR:{e}".encode())
        finally:
            os.close(w_fd)
            os._exit(0)
    else:  # Parent
        os.close(w_fd)
        os.waitpid(pid, 0)
        data = b""
        while chunk := os.read(r_fd, 1024):
            data += chunk
        os.close(r_fd)
        assert data.decode() == "OK", f"Child failed: {data.decode()}"
        # The parent's connection is untouched
        assert cache._get_conn() is parent_conn


def test_item_count_gauge_is_updated_on_purge(time: Mock, db: HotCacheDBConfig) -> None:
    metrics = Mock(spec=BaseMetricsCollector)
    cache = build_cache(
        make_client(),
        db,
        metrics_collector=metrics,
        purge_interval_seconds=0,  # Purge (and update the gauge) on every store
    )

    assert cache.get("foo_hot") == 1  # Expires at 60, dropped at 70
    metrics.gauge_set.assert_called_with("item_count", 1)

    time.time.return_value = 30
    assert cache.get("bar_hot") == 1  # Expires at 90, dropped at 100
    metrics.gauge_set.assert_called_with("item_count", 2)

    # The first entry is past its hard deadline and gets purged, the second
    # one is still servable and stays
    time.time.return_value = 71
    assert cache.get("baz_hot") == 1
    metrics.gauge_set.assert_called_with("item_count", 2)  # bar_hot + baz_hot


def break_db(db: HotCacheDBConfig) -> None:
    """Pull the table out from under a cache that is already connected."""
    conn = sqlite3.connect(db.db_path)
    conn.execute("DROP TABLE hot_cache")
    conn.commit()
    conn.close()


def test_errors_are_counted_and_behave_as_a_miss(
    time: Mock, db: HotCacheDBConfig
) -> None:
    metrics = Mock(spec=BaseMetricsCollector)
    cache = build_cache(make_client(), db, metrics_collector=metrics)

    time.time.return_value = 0
    assert cache.get("foo_hot") == 1
    break_db(db)
    metrics.metric_inc.reset_mock()

    # A lookup that cannot read the db behaves as a miss on a cold key
    assert cache._lookup_hot_cache(Key("foo_hot")) is None
    # A value that cannot be stored is simply not cached
    cache._store_entry(Key("foo_hot"), 1)
    # And a stale entry that cannot be dropped is reported as not dropped
    assert cache._clear_hot_cache_if_necessary(Key("foo_hot")) is False

    assert metrics.metric_inc.call_args_list.count(call("errors")) == 3


def test_a_broken_db_does_not_fail_the_request(
    client: Mock, time: Mock, db: HotCacheDBConfig
) -> None:
    cache = build_cache(client, db)

    time.time.return_value = 0
    assert cache.get("foo_hot") == 1
    break_db(db)

    # The hot cache is best effort: reads still get served by the server
    client.meta_get.reset_mock()
    assert cache.get("foo_hot") == 1
    client.meta_get.assert_called_once()
    assert cache.multi_get(["foo_hot", "foo_miss"]) == {
        Key("foo_hot"): 1,
        Key("foo_miss"): None,
    }


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="sqlite result codes need python 3.11+"
)
def test_only_a_full_db_is_purged(
    time: Mock, db: HotCacheDBConfig, monkeypatch
) -> None:
    cache = build_cache(make_client(), db)

    time.time.return_value = 0
    assert cache.get("foo_hot") == 1
    break_db(db)

    purge = Mock()
    monkeypatch.setattr(cache, "_purge_expired", purge)

    # "no such table" is SQLITE_ERROR, not SQLITE_FULL: purging would not
    # make room for anything, so it is not even attempted.
    cache._store_entry(Key("foo_hot"), 1)
    purge.assert_not_called()


def test_a_full_db_is_purged_and_retried(
    time: Mock, tmp_path: Path, monkeypatch
) -> None:
    db = HotCacheDBConfig.initialize(str(tmp_path / "hot.db"), max_size_bytes=64 * 1024)
    cache = build_cache(make_client(), db)

    time.time.return_value = 0
    blob = b"x" * 4096
    for i in range(50):  # Fill it up
        cache._store_entry(Key(f"key_{i}"), blob)

    # Past the hard deadline, the next store hits SQLITE_FULL, purges, and
    # succeeds on the retry
    time.time.return_value = 100
    purge = Mock(wraps=cache._purge_expired)
    monkeypatch.setattr(cache, "_purge_expired", purge)
    cache._store_entry(Key("fresh"), blob)
    purge.assert_called_once()
    assert cache._lookup_hot_cache(Key("fresh")) == hot(blob)


def test_purge_runs_on_a_schedule(
    client: Mock, time: Mock, db: HotCacheDBConfig
) -> None:
    cache = build_cache(client, db, purge_interval_seconds=100)

    time.time.return_value = 0
    assert cache.get("foo_hot") == 1  # Expires at 60, dropped at 70

    # Past its hard deadline, but the purge is not due yet
    time.time.return_value = 71
    assert cache.get("bar_hot") == 1
    assert row_count(db) == 2

    # The interval is up: the store that follows purges it
    time.time.return_value = 101
    assert cache.get("baz_hot") == 1
    assert row_count(db) == 2  # bar_hot (expires at 131) and baz_hot
    assert cache._lookup_hot_cache(Key("foo_hot")) is None


def test_purge_is_not_repeated_within_the_interval(
    client: Mock, time: Mock, db: HotCacheDBConfig, monkeypatch
) -> None:
    cache = build_cache(client, db, purge_interval_seconds=100)
    time.time.return_value = 0
    assert cache.get("first_hot") == 1  # Connects, so the interval starts here
    purge = Mock(wraps=cache._purge_expired)
    monkeypatch.setattr(cache, "_purge_expired", purge)

    time.time.return_value = 101
    for i in range(10):  # However many stores land in the same interval...
        assert cache.get(f"key_{i}_hot") == 1
    purge.assert_called_once()  # ... only the first one purges

    time.time.return_value = 202
    assert cache.get("later_hot") == 1
    assert purge.call_count == 2


def test_purge_truncates_the_write_ahead_log(time: Mock, tmp_path: Path) -> None:
    db = HotCacheDBConfig.initialize(
        str(tmp_path / "hot.db"), max_size_bytes=1024 * 1024
    )
    cache = build_cache(make_client(), db, purge_interval_seconds=1 << 30)
    wal = tmp_path / "hot.db-wal"

    time.time.return_value = 0
    for i in range(100):
        cache._store_entry(Key(f"key_{i}"), b"x" * 4096)
    assert wal.stat().st_size > 0  # The writes are sitting in the log

    # Purging hands the pages back to the db file and truncates the log
    time.time.return_value = 100  # Everything is past its hard deadline
    cache._purge_expired(cache._get_conn())
    assert row_count(db) == 0
    assert wal.stat().st_size == 0


def test_the_write_ahead_log_is_bounded(tmp_path: Path) -> None:
    db = HotCacheDBConfig.initialize(
        str(tmp_path / "hot.db"), max_size_bytes=1024 * 1024, max_wal_bytes=64 * 1024
    )
    assert db.max_wal_bytes == 64 * 1024

    conn = db.connect()
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    assert conn.execute("PRAGMA journal_size_limit").fetchone()[0] == 64 * 1024
    assert conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == (
        64 * 1024 // page_size
    )
    conn.close()


def test_a_busy_checkpoint_does_not_block_the_store(
    time: Mock, db: HotCacheDBConfig
) -> None:
    cache = build_cache(make_client(), db, purge_interval_seconds=0)

    # Another worker holds a read snapshot, so the log cannot be truncated
    reader = sqlite3.connect(db.db_path)
    reader.execute("BEGIN")
    reader.execute("SELECT COUNT(*) FROM hot_cache").fetchone()
    try:
        time.time.return_value = 0
        # The purge still happens, the checkpoint is simply skipped
        assert cache.get("foo_hot") == 1
        assert row_count(db) == 1
    finally:
        reader.close()


def expiration_of(db: HotCacheDBConfig, key: str) -> Optional[int]:
    conn = sqlite3.connect(db.db_path)
    try:
        row = conn.execute(
            "SELECT expiration FROM hot_cache WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def test_one_worker_extends_the_hot_value_for_everybody(
    time: Mock, db: HotCacheDBConfig
) -> None:
    client_a, client_b = make_client(), make_client()
    worker_a = build_cache(client_a, db, extend_on_error=True)
    worker_b = build_cache(client_b, db, extend_on_error=True)

    assert worker_a.get("foo_hot") == 1

    # The server starts failing right as the value goes stale
    client_a.meta_get.side_effect = MemcacheError("mimic cache error")
    time.time.return_value = 61

    # Worker A hits the error and is served the stale value instead
    assert worker_a.get("foo_hot") == 1
    assert expiration_of(db, "foo_hot") == 121  # Stored again: a fresh ttl

    # And worker B, in another process, never even sees the outage: the
    # extension one worker won is shared with all of them
    assert worker_b.get("foo_hot") == 1
    client_b.meta_get.assert_not_called()


def test_a_slow_failure_can_overwrite_a_concurrent_refresh(
    time: Mock, db: HotCacheDBConfig
) -> None:
    """Known limitation of storing the stale value back on error.

    A worker whose request fails stores back the value it read before
    making it, so a refresh another worker landed in the meantime is
    overwritten with the older one. It is reachable whenever a failing
    request outlives revalidation_retry_seconds, which during an outage is
    the normal case, and it costs a cache_ttl of recovery for that key.
    """
    client_a = make_client()
    worker_a = build_cache(client_a, db, extend_on_error=True)
    worker_b = build_cache(make_client(), db, extend_on_error=True)

    assert worker_a.get("foo_hot") == 1

    def fail_after_a_refresh(key, **kwargs):
        # Worker B is elected on the retry clock and refreshes the value
        # while A's request is still in flight
        worker_b._store_entry(key, 2)
        raise MemcacheError("mimic cache error")

    client_a.meta_get.side_effect = fail_after_a_refresh
    time.time.return_value = 61

    assert worker_a.get("foo_hot") == 1  # A serves the value it read
    assert worker_b._lookup_hot_cache(Key("foo_hot")) == hot(1)  # ... and stored it
