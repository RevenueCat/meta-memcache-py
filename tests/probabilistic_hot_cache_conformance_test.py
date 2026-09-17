"""Behaviour shared by every ProbabilisticHotCache implementation.

Every test here runs against both the in-memory and the sqlite hot cache:
what they promote, what they serve, and above all when a stale value stops
being served. Tests that only make sense for one of them live in
probabilistic_hot_cache_test.py and probabilistic_hot_cache_sqlite_test.py.
"""

from pathlib import Path
from typing import Mapping
from unittest.mock import Mock

import pytest
from prometheus_client import CollectorRegistry

from meta_memcache import Key
from meta_memcache.configuration import LeasePolicy
from meta_memcache.errors import MemcacheError
from meta_memcache.metrics.prometheus import PrometheusMetricsCollector
from meta_memcache.protocol import Miss, ResponseFlags, Value
from tests.hot_cache_harness import (
    DEFAULT_FLAGS,
    HARD_DEADLINE,
    LEASE_FLAGS,
    HotCacheHarness,
    counters,
    make_client,
    make_lease_client,
    memory_harness,
    sqlite_harness,
)


@pytest.fixture(params=["memory", "sqlite"])
def harness(request, tmp_path: Path) -> HotCacheHarness:
    if request.param == "memory":
        return memory_harness()
    return sqlite_harness(str(tmp_path / "hot.db"))


@pytest.fixture
def time(monkeypatch) -> Mock:
    # Both implementations read the clock from their own module.
    time_mock = Mock()
    time_mock.time.return_value = 0
    for module in ("", "_sqlite"):
        monkeypatch.setattr(
            f"meta_memcache.extras.probabilistic_hot_cache{module}.time", time_mock
        )
    return time_mock


@pytest.fixture
def random(monkeypatch) -> Mock:
    # Promotion is decided by the shared base class.
    random_mock = Mock()
    monkeypatch.setattr(
        "meta_memcache.extras.probabilistic_hot_cache.random", random_mock
    )
    return random_mock


@pytest.fixture
def client() -> Mock:
    return make_client()


@pytest.fixture
def lease_client() -> Mock:
    return make_lease_client()


@pytest.fixture
def metrics() -> PrometheusMetricsCollector:
    return PrometheusMetricsCollector(namespace="test", registry=CollectorRegistry())


def assert_counters(
    metrics: PrometheusMetricsCollector, **expected: int
) -> Mapping[str, int]:
    assert counters(metrics) == {
        f"test_hot_cache_{name}": value for name, value in expected.items()
    }


def test_hot_keys_are_cached(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)

    assert cache.get("foo_hot") == 1
    client.meta_get.assert_called_once_with(key=Key("foo_hot"), **DEFAULT_FLAGS)
    assert harness.contains("foo_hot")

    # The second read is served locally
    client.meta_get.reset_mock()
    assert cache.get("foo_hot") == 1
    client.meta_get.assert_not_called()


def test_cold_and_missing_keys_are_not_cached(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)

    assert cache.get("foo") == 1  # Read too long ago to be hot
    assert cache.get("foo_miss") is None
    assert harness.count() == 0

    # And they keep going to the server on every read
    client.meta_get.reset_mock()
    assert cache.get("foo") == 1
    assert cache.get("foo_miss") is None
    assert client.meta_get.call_count == 2


def test_only_one_out_of_probability_factor_is_promoted(
    harness: HotCacheHarness, client: Mock, time: Mock, random: Mock
) -> None:
    cache = harness.build(client, probability_factor=100)

    # A key detected as hot is only promoted when it wins the draw
    random.getrandbits.return_value = 1
    assert cache.get("foo_hot") == 1
    assert not harness.contains("foo_hot")

    random.getrandbits.return_value = 100
    assert cache.get("foo_hot") == 1
    assert harness.contains("foo_hot")


def test_only_allowed_prefixes_are_cached(
    harness: HotCacheHarness, client: Mock, time: Mock, metrics: Mock
) -> None:
    cache = harness.build(
        client,
        allowed_prefixes=["allowed:", "also_allowed:"],
        metrics_collector=metrics,
    )

    # Hot keys under any of the allowed prefixes are promoted
    assert cache.get("allowed:foo_hot") == 1
    assert cache.get("also_allowed:foo_hot") == 1
    assert harness.contains("allowed:foo_hot")
    assert harness.contains("also_allowed:foo_hot")

    # A key that merely starts like an allowed prefix is not covered by it
    assert cache.get("allowed_is_not:foo_hot") == 1
    assert not harness.contains("allowed_is_not:foo_hot")

    # Nor are cold or missing keys, allowed prefix or not
    assert cache.get("allowed:foo") == 1
    assert cache.get("allowed:foo_miss") is None
    assert harness.count() == 2

    assert_counters(
        metrics,
        hits=0,
        misses=4,
        skips=1,
        hot_candidates=2,
        hot_skips=1,
        candidate_misses=1,
    )


def test_multi_get(
    harness: HotCacheHarness, client: Mock, time: Mock, metrics: Mock
) -> None:
    cache = harness.build(
        client, allowed_prefixes=["allowed:"], metrics_collector=metrics
    )
    keys = ["allowed:foo_hot", "allowed:foo", "allowed:foo_miss", "foo_hot", "foo"]
    expected = {
        Key("allowed:foo_hot"): 1,
        Key("allowed:foo"): 1,
        Key("allowed:foo_miss"): None,
        Key("foo_hot"): 1,
        Key("foo"): 1,
    }

    assert cache.multi_get(keys) == expected
    client.meta_multiget.assert_called_once_with(
        keys=[Key(key) for key in keys], **DEFAULT_FLAGS
    )
    # Only the hot key under an allowed prefix is promoted
    assert harness.count() == 1
    assert harness.contains("allowed:foo_hot")

    # The second round returns the same, but the promoted key is served
    # locally and no longer asked for
    client.meta_multiget.reset_mock()
    assert cache.multi_get(keys) == expected
    client.meta_multiget.assert_called_once_with(
        keys=[Key(key) for key in keys if key != "allowed:foo_hot"], **DEFAULT_FLAGS
    )

    assert_counters(
        metrics,
        hits=1,
        misses=5,
        skips=4,
        hot_candidates=1,
        hot_skips=2,
        candidate_misses=2,
    )


def test_expired_value_is_refreshed(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)
    assert cache.get("foo_hot") == 1
    client.meta_get.reset_mock()

    # Still fresh just before the expiration
    time.time.return_value = 59
    assert cache.get("foo_hot") == 1
    client.meta_get.assert_not_called()

    # Stale: the read goes to the server and stores a fresh value...
    time.time.return_value = 60
    assert cache.get("foo_hot") == 1
    client.meta_get.assert_called_once()

    # ... which is served locally again well past the original hard
    # deadline (70), because the refresh reset the clocks (expires at 120)
    client.meta_get.reset_mock()
    time.time.return_value = 100
    assert cache.get("foo_hot") == 1
    client.meta_get.assert_not_called()


def test_a_single_reader_is_elected_to_revalidate(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)
    assert cache.get("foo_hot") == 1

    time.time.return_value = 61  # Stale, within the grace window
    # The first reader is elected: it sees a miss, so it refreshes
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, True, None)
    # Everyone else is served the stale value while it does
    assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)
    assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)


def test_abandoned_revalidation_is_retried_until_the_deadline(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client, revalidation_retry_seconds=3)
    assert cache.get("foo_hot") == 1

    time.time.return_value = 61
    # A reader is elected (retry clock set to 64)... and never comes back
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, True, None)
    assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)

    time.time.return_value = 63  # Not yet: the election still stands
    assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)

    time.time.return_value = 64  # Retry clock reached: a new reader is elected
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, True, None)
    assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)

    # The retries stop at the hard deadline, and the value is dropped
    time.time.return_value = HARD_DEADLINE
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, False, None)
    assert harness.count() == 0


def test_stale_value_expires_if_it_cannot_be_revalidated(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)
    assert cache.get("foo_hot") == 1

    # The server starts failing, so nobody manages to revalidate
    client.meta_get.side_effect = MemcacheError("mimic cache error")

    time.time.return_value = 61  # Stale: a reader is elected...
    with pytest.raises(MemcacheError):
        cache.get("foo_hot")  # ... and fails to refresh
    # The rest are served the stale value while the grace window lasts
    assert cache.get("foo_hot") == 1

    time.time.return_value = HARD_DEADLINE
    # Out of grace: the value is dropped rather than served stale forever,
    # and every reader goes back to the (still failing) server
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, False, None)
    assert harness.count() == 0
    with pytest.raises(MemcacheError):
        cache.get("foo_hot")

    # Once the server recovers, the key is detected as hot again
    client.meta_get.side_effect = make_client().meta_get.side_effect
    assert cache.get("foo_hot") == 1
    assert cache._lookup_hot_cache(Key("foo_hot")) == (True, True, 1)


def test_stale_value_is_not_served_long_past_the_deadline(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)
    assert cache.get("foo_hot") == 1

    # A value nobody read for a while is way past its deadline: it is
    # dropped, rather than revalidated and served to every other reader.
    time.time.return_value = 1000
    assert cache._lookup_hot_cache(Key("foo_hot")) == (False, False, None)
    assert harness.count() == 0


def test_value_deleted_from_the_server_is_dropped(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)
    assert cache.get("foo_hot") == 1
    assert cache.multi_get(["multi_hot"]) == {Key("multi_hot"): 1}
    assert harness.count() == 2

    # The keys get deleted from the server
    client.meta_get.side_effect = lambda key, **kwargs: Miss()
    client.meta_multiget.side_effect = lambda keys, **kwargs: {
        key: Miss() for key in keys
    }

    time.time.return_value = 61  # Stale: the elected reader revalidates...
    assert cache.get("foo_hot") is None  # ... and sees the miss
    assert cache.multi_get(["multi_hot"]) == {Key("multi_hot"): None}
    assert harness.count() == 0  # ... so the stale values are dropped for all


def test_value_no_longer_hot_is_not_cached_again(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)
    assert cache.get("foo_hot") == 1

    # The key stops being read often enough to be hot
    client.meta_get.side_effect = lambda key, **kwargs: Value(
        size=1, value=1, flags=ResponseFlags(fetched=True, last_access=9999)
    )

    time.time.return_value = 1000
    assert cache.get("foo_hot") == 1
    assert harness.count() == 0


def test_values_are_isolated_between_reads(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)
    cache._store_entry(Key("k"), {"a": [1, 2]})

    # A caller mutating what it got back cannot pollute the hot cache
    _, _, value = cache._lookup_hot_cache(Key("k"))
    value["a"].append(3)
    assert cache._lookup_hot_cache(Key("k")) == (True, True, {"a": [1, 2]})


def test_revalidation_retry_seconds_must_be_positive(
    harness: HotCacheHarness, client: Mock
) -> None:
    with pytest.raises(ValueError, match="revalidation_retry_seconds"):
        harness.build(client, revalidation_retry_seconds=0)


def test_get_or_lease_uses_the_hot_cache(
    harness: HotCacheHarness, lease_client: Mock, time: Mock
) -> None:
    cache = harness.build(lease_client)
    lease_policy = LeasePolicy()

    # A cold key is fetched from the server and not promoted
    assert cache.get_or_lease("foo_cold", lease_policy=lease_policy) == 1
    assert not harness.contains("foo_cold")
    lease_client.meta_get.assert_called_once_with(key=Key("foo_cold"), **LEASE_FLAGS)

    # A hot key is fetched from the server and promoted
    lease_client.meta_get.reset_mock()
    assert cache.get_or_lease("foo_hot", lease_policy=lease_policy) == 1
    assert harness.contains("foo_hot")
    lease_client.meta_get.assert_called_once_with(key=Key("foo_hot"), **LEASE_FLAGS)

    time.time.return_value = 10
    lease_client.meta_get.reset_mock()

    # The cold key still goes to the server every time
    assert cache.get_or_lease("foo_cold", lease_policy=lease_policy) == 1
    lease_client.meta_get.assert_called_once_with(key=Key("foo_cold"), **LEASE_FLAGS)

    # The hot key is served locally: no lease needed, no call to the server
    lease_client.meta_get.reset_mock()
    assert cache.get_or_lease("foo_hot", lease_policy=lease_policy) == 1
    lease_client.meta_get.assert_not_called()

    # Once the local copy goes stale, it is refreshed through the lease path
    time.time.return_value = 60
    assert cache.get_or_lease("foo_hot", lease_policy=lease_policy) == 1
    lease_client.meta_get.assert_called_once_with(key=Key("foo_hot"), **LEASE_FLAGS)

    # And the refresh reset the clocks: still local past the original
    # hard deadline (70)
    lease_client.meta_get.reset_mock()
    time.time.return_value = 100
    assert cache.get_or_lease("foo_hot", lease_policy=lease_policy) == 1
    lease_client.meta_get.assert_not_called()


def test_get_or_lease_does_not_promote_lease_placeholders(
    harness: HotCacheHarness, lease_client: Mock, time: Mock
) -> None:
    cache = harness.build(lease_client)

    # We won the lease: behaves as a miss, and the empty placeholder we
    # planted on the server must not be cached locally.
    assert cache.get_or_lease("foo_win", lease_policy=LeasePolicy()) is None
    assert not harness.contains("foo_win")
    lease_client.meta_get.reset_mock()

    # We lost the lease and ran out of retries: same, even though the
    # placeholder looks hot to the promotion heuristic.
    wait = Mock()
    assert (
        cache.get_or_lease(
            "foo_lost",
            lease_policy=LeasePolicy(miss_retries=2),
            lease_wait_fn=wait,
        )
        is None
    )
    assert not harness.contains("foo_lost")
    assert lease_client.meta_get.call_count == 2
    wait.assert_called_once_with(1.0)


def test_get_or_lease_elects_a_single_refresher(
    harness: HotCacheHarness, lease_client: Mock, time: Mock
) -> None:
    cache = harness.build(lease_client)
    lease_policy = LeasePolicy()

    assert cache.get_or_lease("foo_hot", lease_policy=lease_policy) == 1
    lease_client.meta_get.reset_mock()

    # The local copy is stale but within the grace window: the first caller
    # is elected to refresh (and takes the lease), the second serves the
    # stale value without hitting the server.
    time.time.return_value = 65
    assert cache.get_or_lease("foo_hot", lease_policy=lease_policy) == 1
    lease_client.meta_get.assert_called_once_with(key=Key("foo_hot"), **LEASE_FLAGS)

    lease_client.meta_get.reset_mock()
    assert cache.get_or_lease("foo_hot", lease_policy=lease_policy) == 1
    lease_client.meta_get.assert_not_called()


def test_get_or_lease_cas_bypasses_the_hot_cache(
    harness: HotCacheHarness, lease_client: Mock, time: Mock
) -> None:
    cache = harness.build(lease_client)

    assert cache.get_or_lease("foo_hot", lease_policy=LeasePolicy()) == 1
    assert harness.contains("foo_hot")
    lease_client.meta_get.reset_mock()

    # A CAS token is only meaningful coming from the server, so the local
    # copy is not used, same as get_cas().
    assert cache.get_or_lease_cas("foo_hot", lease_policy=LeasePolicy()) == (1, None)
    lease_client.meta_get.assert_called_once_with(key=Key("foo_hot"), **LEASE_FLAGS)
