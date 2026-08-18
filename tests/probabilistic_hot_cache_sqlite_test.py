import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import Mock

import pytest

from meta_memcache import CacheClient, Key, Value
from meta_memcache.extras.probabilistic_hot_cache_sqlite import (
    HotCacheDBConfig,
    SqliteProbabilisticHotCache,
)
from meta_memcache.interfaces.router import DEFAULT_FAILURE_HANDLING, FailureHandling
from meta_memcache.metrics.base import BaseMetricsCollector
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
        purge_probability_factor=1 << 30,
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
        "INSERT INTO hot_cache (key, value, expiration) VALUES ('k', x'00', 100)"
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
    # entry (and its extension to 71) would be expired, so a hit without a
    # new revalidation proves the refresh stored a fresh value
    time.time.return_value = 75
    assert worker_b.get("foo_hot") == 1
    client_b.meta_get.assert_not_called()


def test_stale_value_keeps_being_revalidated(
    client: Mock, time: Mock, db: HotCacheDBConfig
) -> None:
    cache = build_cache(client, db)

    time.time.return_value = 0
    assert cache.get("foo_hot") == 1

    # Long past expiration (60) + stale window (10), the entry is not
    # dropped: a winner is elected to revalidate, the rest serve stale
    time.time.return_value = 1000
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, True, None)
    assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)
    assert row_count(db) == 1


def test_abandoned_revalidation_is_retried(time: Mock, db: HotCacheDBConfig) -> None:
    worker_a = build_cache(make_client(), db)
    worker_b = build_cache(make_client(), db)

    time.time.return_value = 0
    assert worker_a.get("foo_hot") == 1

    time.time.return_value = 61
    # Worker A wins the revalidation (next check time set to 71)... and dies
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == (False, True, None)
    # Meanwhile everybody else serves the stale value
    assert worker_b._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)

    time.time.return_value = 71  # Next check time: a new winner retries
    assert worker_b._lookup_hot_cache(Key("foo_hot")) == (False, True, None)
    assert worker_a._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)


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
        purge_probability_factor=1,  # Purge (and update gauges) on every store
    )
    assert cache.get("foo_hot") == 1
    metrics.metric_inc.assert_any_call("misses")
    metrics.metric_inc.assert_any_call("hot_candidates")
    metrics.gauge_set.assert_called_with("item_count", 1)

    assert cache.get("foo_hot") == 1
    metrics.metric_inc.assert_any_call("hits")
