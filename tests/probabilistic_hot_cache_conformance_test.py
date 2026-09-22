"""Behaviour shared by every ProbabilisticHotCache implementation.

Every test here runs against both the in-memory and the sqlite hot cache:
what they promote, what they serve, and above all when a stale value stops
being served. Tests that only make sense for one of them live in
probabilistic_hot_cache_test.py and probabilistic_hot_cache_sqlite_test.py.
"""

from pathlib import Path
from typing import Callable, Mapping
from unittest.mock import Mock

import pytest
from prometheus_client import CollectorRegistry

from meta_memcache import Key
from meta_memcache.configuration import LeasePolicy
from meta_memcache.errors import MemcacheError
from meta_memcache.metrics.prometheus import PrometheusMetricsCollector
from meta_memcache.interfaces.router import DEFAULT_FAILURE_HANDLING
from meta_memcache.protocol import MISS_DUE_TO_ERROR, Miss, ResponseFlags, Value
from tests.hot_cache_harness import (
    hot,
    revalidating,
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
        error_extensions=0,
        listed_promotions=0,
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
        error_extensions=0,
        listed_promotions=0,
    )


def test_listed_keys_are_promoted_on_first_read(
    harness: HotCacheHarness, client: Mock, time: Mock, random: Mock, metrics: Mock
) -> None:
    cache = harness.build(
        client,
        hot_keys=["foo", "foo_miss"],
        probability_factor=100,
        metrics_collector=metrics,
    )

    # The server says it was read too long ago to be hot: stored anyway,
    # and without tossing the coin
    assert cache.get("foo") == 1
    assert harness.contains("foo")
    random.getrandbits.assert_not_called()

    # The second read is served locally
    client.meta_get.reset_mock()
    assert cache.get("foo") == 1
    client.meta_get.assert_not_called()

    # An unlisted key is left to the server's signal
    assert cache.get("bar") == 1
    assert not harness.contains("bar")

    # A listed key the server does not have is nothing to store
    assert cache.get("foo_miss") is None
    assert not harness.contains("foo_miss")

    assert_counters(
        metrics,
        hits=1,
        misses=3,
        skips=0,
        hot_candidates=0,
        hot_skips=0,
        candidate_misses=1,
        error_extensions=0,
        listed_promotions=1,
    )


def test_listed_keys_still_need_an_allowed_prefix(
    harness: HotCacheHarness, client: Mock, time: Mock, metrics: Mock
) -> None:
    cache = harness.build(
        client,
        hot_keys=["foo", "allowed:foo"],
        allowed_prefixes=["allowed:"],
        metrics_collector=metrics,
    )

    assert cache.get("allowed:foo") == 1
    assert harness.contains("allowed:foo")

    # The list says which keys are hot; allowed_prefixes still says which
    # keys may be served stale
    assert cache.get("foo") == 1
    assert not harness.contains("foo")

    assert_counters(
        metrics,
        hits=0,
        misses=1,
        skips=1,
        hot_candidates=0,
        hot_skips=1,
        candidate_misses=0,
        error_extensions=0,
        listed_promotions=1,
    )


def test_listed_keys_are_promoted_in_multi_get(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client, hot_keys=["foo"])
    expected = {Key("foo"): 1, Key("bar"): 1}

    assert cache.multi_get(["foo", "bar"]) == expected
    assert harness.contains("foo")
    assert not harness.contains("bar")

    # The listed key is served locally and no longer asked for
    client.meta_multiget.reset_mock()
    assert cache.multi_get(["foo", "bar"]) == expected
    client.meta_multiget.assert_called_once_with(keys=[Key("bar")], **DEFAULT_FLAGS)


def test_set_hot_keys_replaces_the_list(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client, hot_keys=["foo"])

    cache.set_hot_keys(["bar"])
    assert cache.get("foo") == 1
    assert cache.get("bar") == 1
    assert not harness.contains("foo")
    assert harness.contains("bar")

    cache.set_hot_keys(())
    assert cache.get("baz") == 1
    assert harness.count() == 1


def test_hot_keys_rejects_a_bare_string(harness: HotCacheHarness, client: Mock) -> None:
    with pytest.raises(TypeError, match="hot_keys"):
        harness.build(client, hot_keys="foo")

    cache = harness.build(client)
    with pytest.raises(TypeError, match="hot_keys"):
        cache.set_hot_keys("foo")


def test_dropped_listed_key_is_promoted_again_on_the_next_read(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client, hot_keys=["foo"])
    assert cache.get("foo") == 1

    # The key is gone from the server: the stale value is dropped
    client.meta_get.side_effect = lambda key, **kwargs: Miss()
    time.time.return_value = 61
    assert cache.get("foo") is None
    assert harness.count() == 0

    # Back on the server: stored again on the very next read, without
    # waiting for the server to call it hot
    client.meta_get.side_effect = make_client().meta_get.side_effect
    assert cache.get("foo") == 1
    assert harness.contains("foo")


def test_get_or_lease_promotes_listed_keys(
    harness: HotCacheHarness, lease_client: Mock, time: Mock
) -> None:
    cache = harness.build(lease_client, hot_keys=["foo_cold", "foo_win"])

    assert cache.get_or_lease("foo_cold", lease_policy=LeasePolicy()) == 1
    assert harness.contains("foo_cold")

    # A lease placeholder is a miss, listed or not
    assert cache.get_or_lease("foo_win", lease_policy=LeasePolicy()) is None
    assert not harness.contains("foo_win")


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
    assert cache._lookup_hot_cache(Key("foo_hot")) == revalidating(1)
    # Everyone else is served the stale value while it does
    assert cache._lookup_hot_cache(Key("foo_hot")) == hot(1)
    assert cache._lookup_hot_cache(Key("foo_hot")) == hot(1)


def test_abandoned_revalidation_is_retried_until_the_deadline(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client, revalidation_retry_seconds=3)
    assert cache.get("foo_hot") == 1

    time.time.return_value = 61
    # A reader is elected (retry clock set to 64)... and never comes back
    assert cache._lookup_hot_cache(Key("foo_hot")) == revalidating(1)
    assert cache._lookup_hot_cache(Key("foo_hot")) == hot(1)

    time.time.return_value = 63  # Not yet: the election still stands
    assert cache._lookup_hot_cache(Key("foo_hot")) == hot(1)

    time.time.return_value = 64  # Retry clock reached: a new reader is elected
    assert cache._lookup_hot_cache(Key("foo_hot")) == revalidating(1)
    assert cache._lookup_hot_cache(Key("foo_hot")) == hot(1)

    # The retries stop at the hard deadline, and the value is dropped
    time.time.return_value = HARD_DEADLINE
    assert cache._lookup_hot_cache(Key("foo_hot")) is None
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
    assert cache._lookup_hot_cache(Key("foo_hot")) is None
    assert harness.count() == 0
    with pytest.raises(MemcacheError):
        cache.get("foo_hot")

    # Once the server recovers, the key is detected as hot again
    client.meta_get.side_effect = make_client().meta_get.side_effect
    assert cache.get("foo_hot") == 1
    assert cache._lookup_hot_cache(Key("foo_hot")) == hot(1)


def test_stale_value_is_not_served_long_past_the_deadline(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client)
    assert cache.get("foo_hot") == 1

    # A value nobody read for a while is way past its deadline: it is
    # dropped, rather than revalidated and served to every other reader.
    time.time.return_value = 1000
    assert cache._lookup_hot_cache(Key("foo_hot")) is None
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
    cache._lookup_hot_cache(Key("k")).value["a"].append(3)
    assert cache._lookup_hot_cache(Key("k")) == hot({"a": [1, 2]})


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


@pytest.fixture(params=["a pool that raises", "a pool that does not raise"])
def break_the_server(request) -> Callable[[Mock], None]:
    """Both shapes a server failure arrives in.

    A pool with raise_on_server_error raises MemcacheError; one without it
    answers with a miss flagged as an error. The hot cache has to treat the
    two the same, and neither as "this key is gone".
    """

    def apply(client: Mock) -> None:
        if request.param == "a pool that raises":
            error = MemcacheError("mimic cache error")
            client.meta_get.side_effect = error
            client.meta_multiget.side_effect = error
        else:
            client.meta_get.side_effect = lambda *args, **kwargs: MISS_DUE_TO_ERROR
            client.meta_multiget.side_effect = lambda keys, **kwargs: {
                key: MISS_DUE_TO_ERROR for key in keys
            }

    return apply


def read(cache, key: str):
    """What the caller ends up with, whichever way the failure arrived."""
    try:
        return cache.get(key)
    except MemcacheError:
        return None


def test_hot_value_is_served_while_the_server_fails(
    harness: HotCacheHarness, client: Mock, time: Mock, break_the_server
) -> None:
    cache = harness.build(client, extend_on_error=True)
    assert cache.get("foo_hot") == 1  # Expires at 60, dropped at 70

    break_the_server(client)

    # The caller that hits the failure is served the stale value rather than
    # being sent to whatever sits behind the cache
    time.time.return_value = 61
    assert cache.get("foo_hot") == 1

    # And the value is still there past the deadline it would have died at
    # (70), because storing it again bought it another cache_ttl
    client.meta_get.reset_mock()
    time.time.return_value = 80
    assert cache.get("foo_hot") == 1
    client.meta_get.assert_not_called()

    # It goes stale again, and the failure buys it yet another one: an
    # outage never drains the hot cache
    time.time.return_value = 122
    assert cache.get("foo_hot") == 1
    client.meta_get.assert_called()

    # Once the server answers again it is refreshed for real, and from then
    # on it expires normally
    client.meta_get.side_effect = make_client().meta_get.side_effect
    time.time.return_value = 183
    assert cache.get("foo_hot") == 1
    assert cache._lookup_hot_cache(Key("foo_hot")) == hot(1)


def test_hot_value_is_not_rescued_without_the_opt_in(
    harness: HotCacheHarness, client: Mock, time: Mock, break_the_server
) -> None:
    cache = harness.build(client)  # extend_on_error_seconds defaults to 0

    assert cache.get("foo_hot") == 1
    break_the_server(client)

    time.time.return_value = 61
    assert read(cache, "foo_hot") is None

    # Nothing extended it, so it is gone once the grace window runs out and
    # every caller goes to the server
    time.time.return_value = HARD_DEADLINE
    assert cache._lookup_hot_cache(Key("foo_hot")) is None
    assert harness.count() == 0


def test_a_miss_from_a_server_that_answered_still_drops_the_value(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client, extend_on_error=True)
    assert cache.get("foo_hot") == 1

    # A healthy server reporting the key gone is not an outage: extending
    # on error must not turn a deletion into an immortal value
    client.meta_get.side_effect = lambda key, **kwargs: Miss()
    time.time.return_value = 61
    assert cache.get("foo_hot") is None
    assert harness.count() == 0


def test_multi_get_keeps_the_results_of_the_servers_that_answered(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    """One server down must not cost us the keys the others answered."""
    cache = harness.build(client, extend_on_error=True)
    keys = ["down_hot", "up_hot", "down_cold", "up_cold"]
    assert cache.multi_get(keys) == {Key(key): 1 for key in keys}
    assert harness.count() == 2  # Only the hot ones were promoted

    def one_server_down(keys, flags=None, failure_handling=DEFAULT_FAILURE_HANDLING):
        return {
            key: (
                MISS_DUE_TO_ERROR
                if key.key.startswith("down_")
                else Value(
                    size=1, value=2, flags=ResponseFlags(fetched=True, last_access=1)
                )
            )
            for key in keys
        }

    client.meta_multiget.side_effect = one_server_down
    time.time.return_value = 61

    assert cache.multi_get(keys) == {
        Key("down_hot"): 1,  # Stale value kept, rather than dropped
        Key("down_cold"): None,  # Nothing to fall back on
        Key("up_hot"): 2,  # The server that answered refreshed it
        Key("up_cold"): 2,  # ... and this one was never hot
    }

    # The key behind the failing server was kept, the other one refreshed,
    # so both outlive the deadline they started with (70)
    time.time.return_value = 100
    assert cache._lookup_hot_cache(Key("down_hot")) == hot(1)
    assert cache._lookup_hot_cache(Key("up_hot")) == hot(2)


def test_multi_get_keeps_its_hot_values_when_the_whole_batch_fails(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client, extend_on_error=True)
    assert cache.multi_get(["one_hot", "two_hot", "cold"]) == {
        Key("one_hot"): 1,
        Key("two_hot"): 1,
        Key("cold"): 1,
    }

    # A pool that raises loses the whole batch when any of its servers is
    # down, so the hot values are all we have left
    client.meta_multiget.side_effect = MemcacheError("mimic cache error")
    time.time.return_value = 61
    assert cache.multi_get(["one_hot", "two_hot", "cold"]) == {
        Key("one_hot"): 1,
        Key("two_hot"): 1,
        Key("cold"): None,
    }

    time.time.return_value = 100
    assert cache._lookup_hot_cache(Key("one_hot")) == hot(1)


def test_multi_get_raises_when_there_is_nothing_to_rescue(
    harness: HotCacheHarness, client: Mock, time: Mock
) -> None:
    cache = harness.build(client, extend_on_error=True)
    client.meta_multiget.side_effect = MemcacheError("mimic cache error")
    with pytest.raises(MemcacheError):
        cache.multi_get(["cold"])


def test_error_extensions_are_counted(
    harness: HotCacheHarness, client: Mock, time: Mock, metrics: Mock, break_the_server
) -> None:
    cache = harness.build(client, extend_on_error=True, metrics_collector=metrics)
    assert cache.get("foo_hot") == 1

    break_the_server(client)
    time.time.return_value = 61
    assert cache.get("foo_hot") == 1

    assert counters(metrics)["test_hot_cache_error_extensions"] == 1
