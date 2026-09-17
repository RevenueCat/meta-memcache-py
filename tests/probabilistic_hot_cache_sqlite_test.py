import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import Mock, call

import pytest

from meta_memcache import CacheClient, Key, Value
from meta_memcache.extras.probabilistic_hot_cache_sqlite import (
    HotCacheDBConfig,
    SqliteProbabilisticHotCache,
)
from meta_memcache.interfaces.router import DEFAULT_FAILURE_HANDLING, FailureHandling
from meta_memcache.metrics.base import BaseMetricsCollector
from meta_memcache.errors import MemcacheError
from meta_memcache.protocol import Miss, ReadResponse, RequestFlags, ResponseFlags


def make_client() -> Mock:
    def meta_get(
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> ReadResponse:
        if key.key.endswith("hot"):
            return Value(
                size=1,
                value=1,
                flags=ResponseFlags(
                    fetched=True,
                    last_access=1,
                ),
            )
        elif key.key.endswith("miss"):
            return Miss()
        else:
            return Value(
                size=1,
                value=1,
                flags=ResponseFlags(
                    fetched=True,
                    last_access=9999,
                ),
            )

    def meta_multiget(
        keys: List[Key],
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> Dict[Key, ReadResponse]:
        return {key: meta_get(key=key) for key in keys}

    mock = Mock(spec=CacheClient)
    mock.meta_get.side_effect = meta_get
    mock.meta_multiget.side_effect = meta_multiget
    return mock


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
    client: Mock, db: HotCacheDBConfig, **kwargs
) -> SqliteProbabilisticHotCache:
    defaults = dict(
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        purge_interval_seconds=1 << 30,  # Never, unless a test asks for it
    )
    defaults.update(kwargs)
    return SqliteProbabilisticHotCache(client=client, db=db, **defaults)


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


def test_config_validates_initialized_db(db: HotCacheDBConfig) -> None:
    # Workers that didn't run initialize() build the config themselves
    assert HotCacheDBConfig(db.db_path) == HotCacheDBConfig(db.db_path)


def test_hot_keys_are_cached(client: Mock, time: Mock, db: HotCacheDBConfig) -> None:
    cache = build_cache(client, db)
    assert cache.get("foo_hot") == 1
    client.meta_get.assert_called_once()

    client.meta_get.reset_mock()
    assert cache.get("foo_hot") == 1
    client.meta_get.assert_not_called()


def test_cold_keys_are_not_cached(
    client: Mock, time: Mock, db: HotCacheDBConfig
) -> None:
    cache = build_cache(client, db)
    assert cache.get("foo") == 1
    assert cache.get("foo_miss") is None
    client.meta_get.reset_mock()
    assert cache.get("foo") == 1
    assert cache.get("foo_miss") is None
    assert client.meta_get.call_count == 2


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

    time.time.return_value = 0
    assert worker_a.get("foo_hot") == 1  # Stored, expires at 60

    time.time.return_value = 61  # Expired, within the stale window
    # First worker wins the revalidation: sees a miss so it will refresh
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == (False, True, None)
    # Everyone else is served the stale value while the winner refreshes,
    # including other lookups from the winner's own process
    assert worker_b._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)


def test_winner_refreshes_the_value(time: Mock, db: HotCacheDBConfig) -> None:
    client_a, client_b = make_client(), make_client()
    worker_a = build_cache(client_a, db)
    worker_b = build_cache(client_b, db)

    time.time.return_value = 0
    assert worker_a.get("foo_hot") == 1
    client_a.meta_get.reset_mock()

    time.time.return_value = 61  # Expired, within the stale window
    assert worker_a.get("foo_hot") == 1  # Wins and refreshes from the server
    client_a.meta_get.assert_called_once()

    # The refreshed value is fresh again for everybody: at t=75 the original
    # entry would be long past its grace window (60 + 10), so a hit without a
    # new revalidation proves the refresh stored a fresh value
    time.time.return_value = 75
    assert worker_b.get("foo_hot") == 1
    client_b.meta_get.assert_not_called()


def test_stale_value_expires_if_not_revalidated(
    time: Mock, db: HotCacheDBConfig
) -> None:
    client = make_client()
    cache = build_cache(client, db)

    time.time.return_value = 0
    assert cache.get("foo_hot") == 1

    # The server starts failing, so nobody manages to revalidate
    client.meta_get.side_effect = MemcacheError("mimic cache error")

    time.time.return_value = 61  # Expired: a winner is elected...
    with pytest.raises(MemcacheError):
        cache.get("foo_hot")  # ... and fails to refresh
    # The rest keep being served the stale value while the grace window lasts
    assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)

    time.time.return_value = 70  # Grace window (60 + 10) is over
    # The stale value is no longer served, and the key is no longer hot
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, False, None)
    assert row_count(db) == 0

    # Once the server recovers, the key is detected as hot again
    client.meta_get.side_effect = make_client().meta_get.side_effect
    assert cache.get("foo_hot") == 1
    assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)


def test_stale_value_is_not_served_long_past_expiration(
    client: Mock, time: Mock, db: HotCacheDBConfig
) -> None:
    cache = build_cache(client, db)

    time.time.return_value = 0
    assert cache.get("foo_hot") == 1

    # A key nobody read for a while is way past expiration (60) + grace
    # window (10): it is dropped rather than served to everyone but the
    # elected revalidator.
    time.time.return_value = 1000
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, False, None)
    assert row_count(db) == 0


def test_abandoned_revalidation_is_retried(time: Mock, db: HotCacheDBConfig) -> None:
    worker_a = build_cache(make_client(), db, revalidation_retry_seconds=3)
    worker_b = build_cache(make_client(), db, revalidation_retry_seconds=3)

    time.time.return_value = 0
    assert worker_a.get("foo_hot") == 1

    time.time.return_value = 61
    # Worker A wins the revalidation (retry clock set to 64)... and dies
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == (False, True, None)
    # Meanwhile everybody else serves the stale value
    assert worker_b._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)

    time.time.return_value = 64  # Retry clock reached: a new winner retries
    assert worker_b._lookup_hot_cache(Key("foo_hot")) == (False, True, None)
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)

    # The retries stop at the hard deadline: expiration (60) + grace (10)
    time.time.return_value = 70
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == (False, False, None)
    assert row_count(db) == 0


def test_revalidation_retry_seconds_must_be_positive(
    client: Mock, db: HotCacheDBConfig
) -> None:
    with pytest.raises(ValueError, match="revalidation_retry_seconds"):
        build_cache(client, db, revalidation_retry_seconds=0)


def test_deleted_value_is_dropped_on_revalidation(
    time: Mock, db: HotCacheDBConfig
) -> None:
    client = make_client()
    cache = build_cache(client, db)

    time.time.return_value = 0
    assert cache.get("foo_hot") == 1

    # The key gets deleted from the server
    client.meta_get.side_effect = lambda key, **kwargs: Miss()

    time.time.return_value = 61  # Expired: the winner revalidates
    assert cache.get("foo_hot") is None  # ... sees the miss
    assert row_count(db) == 0  # ... and drops the stale entry for everybody


def test_max_size_is_enforced_best_effort(time: Mock, tmp_path: Path) -> None:
    db = HotCacheDBConfig.initialize(str(tmp_path / "hot.db"), max_size_bytes=64 * 1024)
    cache = build_cache(make_client(), db)

    time.time.return_value = 0
    blob = b"x" * 4096
    for i in range(50):
        cache._store_entry(Key(f"key_{i}"), blob)  # Never raises when full

    stored = row_count(db)
    assert 0 < stored < 50  # Capped: some stores were skipped
    assert (tmp_path / "hot.db").stat().st_size <= 64 * 1024

    # Once entries expire, storing purges them and succeeds again
    time.time.return_value = 100  # Beyond expiration (60) + stale window (10)
    cache._store_entry(Key("fresh"), blob)
    assert cache._lookup_hot_cache(Key("fresh")) == (True, True, blob)
    assert row_count(db) < stored


def test_values_are_isolated_between_reads(time: Mock, db: HotCacheDBConfig) -> None:
    cache = build_cache(make_client(), db)
    cache._store_entry(Key("k"), {"a": [1, 2]})
    _, _, value = cache._lookup_hot_cache(Key("k"))
    value["a"].append(3)
    assert cache._lookup_hot_cache(Key("k")) == (True, True, {"a": [1, 2]})


def test_multi_get(client: Mock, time: Mock, db: HotCacheDBConfig) -> None:
    cache = build_cache(client, db)
    expected = {Key("foo_hot"): 1, Key("foo_miss"): None, Key("foo"): 1}
    assert cache.multi_get(["foo_hot", "foo_miss", "foo"]) == expected

    client.meta_multiget.reset_mock()
    assert cache.multi_get(["foo_hot", "foo_miss", "foo"]) == expected
    # The hot key is served from the hot cache, the rest hit the server
    requested_keys = client.meta_multiget.call_args.kwargs["keys"]
    assert Key("foo_hot") not in requested_keys
    assert Key("foo_miss") in requested_keys
    assert Key("foo") in requested_keys


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
            assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)
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


def test_metrics(time: Mock, db: HotCacheDBConfig) -> None:
    metrics = Mock(spec=BaseMetricsCollector)
    cache = build_cache(
        make_client(),
        db,
        metrics_collector=metrics,
        purge_interval_seconds=0,  # Purge (and update gauges) on every store
    )
    assert cache.get("foo_hot") == 1
    metrics.metric_inc.assert_any_call("misses")
    metrics.metric_inc.assert_any_call("hot_candidates")
    metrics.gauge_set.assert_called_with("item_count", 1)

    assert cache.get("foo_hot") == 1
    metrics.metric_inc.assert_any_call("hits")


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
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, False, None)
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
    assert cache._lookup_hot_cache(Key("fresh")) == (True, True, blob)


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
    assert not cache._lookup_hot_cache(Key("foo_hot"))[1]


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
