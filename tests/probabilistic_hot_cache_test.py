from typing import Dict, List, Optional
from unittest.mock import Mock

from prometheus_client import CollectorRegistry
from meta_memcache.interfaces.router import DEFAULT_FAILURE_HANDLING, FailureHandling

import pytest

from meta_memcache import CacheClient, Key, Value
from meta_memcache.errors import MemcacheError
from meta_memcache.extras.probabilistic_hot_cache import (
    CachedValue,
    ProbabilisticHotCache,
)
from meta_memcache.metrics.prometheus import PrometheusMetricsCollector
from meta_memcache.protocol import Miss, ReadResponse, ResponseFlags, RequestFlags


@pytest.fixture
def client() -> Mock:
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
def time(monkeypatch) -> Mock:
    time_mock = Mock()
    monkeypatch.setattr("meta_memcache.extras.probabilistic_hot_cache.time", time_mock)
    return time_mock


@pytest.fixture
def random(monkeypatch) -> Mock:
    random_mock = Mock()
    monkeypatch.setattr(
        "meta_memcache.extras.probabilistic_hot_cache.random", random_mock
    )
    return random_mock


DEFAULT_FLAGS = {
    "flags": RequestFlags(
        return_ttl=True,
        return_last_access=True,
        return_value=True,
        return_fetched=True,
        return_client_flag=True,
    ),
    "failure_handling": DEFAULT_FAILURE_HANDLING,
}


def test_get_without_prefixes(
    time: Mock,
    client: Mock,
) -> None:
    store = {}
    metrics_collector = PrometheusMetricsCollector(
        namespace="test", registry=CollectorRegistry()
    )
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=None,
        metrics_collector=metrics_collector,
        immutable_types=[],
    )

    time.time.return_value = 0

    # Request a key that is cold, it is not stored in the hot cache
    assert hot_cache.get(key="foo_cold") == 1
    assert "foo_cold" not in store
    client.meta_get.assert_called_once_with(key=Key(key="foo_cold"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    # Request a key that is hot, it is stored in the hot cache
    assert hot_cache.get(key="foo_hot") == 1
    assert store.get("foo_hot") == CachedValue(value=1, expiration=60, extended=False)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    # Time goes by
    time.time.return_value = 10

    # A second call to the cold key still causes a cache get from the server
    assert hot_cache.get(key="foo_cold") == 1
    assert "foo_cold" not in store
    client.meta_get.assert_called_once_with(key=Key(key="foo_cold"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    # A second call to the hot key fetches from the hot cache, no call to the server
    assert hot_cache.get(key="foo_hot") == 1
    assert store.get("foo_hot") == CachedValue(value=1, expiration=60, extended=False)
    client.meta_get.assert_not_called()

    # Time goes by
    time.time.return_value = 60

    # The hot cache is now expired, a call to the hot key causes a cache get
    assert hot_cache.get(key="foo_hot") == 1
    assert store.get("foo_hot") == CachedValue(value=1, expiration=120, extended=False)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    # Time goes by
    time.time.return_value = 120

    # mimic cache error, so we can see the hot cache extended by the winner thread
    original_meta_get = client.meta_get.side_effect
    client.meta_get.side_effect = MemcacheError("mimic cache error")

    with pytest.raises(MemcacheError):
        hot_cache.get(key="foo_hot")
    assert store.get("foo_hot") == CachedValue(value=1, expiration=130, extended=True)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    # mimic a second thread, that should use the stale value

    assert hot_cache.get(key="foo_hot") == 1
    assert store.get("foo_hot") == CachedValue(value=1, expiration=130, extended=True)
    client.meta_get.assert_not_called()
    client.meta_get.reset_mock()

    # Time goes by
    time.time.return_value = 130

    # The hot cache is now expired, and has been already extended. The hot cache
    # will not be used and the hot cache is no longer extended.
    with pytest.raises(MemcacheError):
        hot_cache.get(key="foo_hot")
    assert store.get("foo_hot") == CachedValue(value=1, expiration=130, extended=True)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    with pytest.raises(MemcacheError):
        hot_cache.get(key="foo_hot")
    assert store.get("foo_hot") == CachedValue(value=1, expiration=130, extended=True)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    # restore the original meta_get, and check hot cache is again updated and used
    client.meta_get.side_effect = original_meta_get

    assert hot_cache.get(key="foo_hot") == 1
    assert store.get("foo_hot") == CachedValue(value=1, expiration=190, extended=False)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    assert hot_cache.get(key="foo_hot") == 1
    client.meta_get.assert_not_called()

    assert metrics_collector.get_counters() == {
        "test_hot_cache_hits": 3,
        "test_hot_cache_misses": 8,
        "test_hot_cache_skips": 0,
        "test_hot_cache_item_count": 1,
        "test_hot_cache_hot_candidates": 2,
        "test_hot_cache_hot_skips": 0,
        "test_hot_cache_candidate_misses": 0,
    }


def test_get_miss(
    time: Mock,
    client: Mock,
) -> None:
    store = {}
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=None,
    )

    time.time.return_value = 0

    # Request a key that is a miss. It will not be cached
    assert hot_cache.get(key="foo_miss") is None
    assert "foo_miss" not in store
    client.meta_get.assert_called_once_with(key=Key(key="foo_miss"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    assert hot_cache.get(key="foo_miss") is None
    assert "foo_miss" not in store
    client.meta_get.assert_called_once_with(key=Key(key="foo_miss"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()


def test_get_prefixes(
    time: Mock,
    client: Mock,
) -> None:
    store = {}
    metrics_collector = PrometheusMetricsCollector(
        namespace="test", registry=CollectorRegistry()
    )
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=["allowed:", "also_allowed:"],
        metrics_collector=metrics_collector,
    )

    time.time.return_value = 0

    assert hot_cache.get(key="allowed_is_not:foo_hot") == 1
    assert "allowed_is_not:foo_hot" not in store

    assert hot_cache.get(key="allowed_is_not:foo_cold") == 1
    assert "allowed_is_not:foo_cold" not in store

    assert hot_cache.get(key="allowed_is_not:foo_miss") is None
    assert "allowed_is_not:foo_miss" not in store

    assert hot_cache.get(key="allowed:foo_hot") == 1
    assert "allowed:foo_hot" in store  # is hot and allowed

    assert hot_cache.get(key="allowed:foo_cold") == 1
    assert "allowed:foo_cold" not in store

    assert hot_cache.get(key="allowed:foo_miss") is None
    assert "allowed:foo_miss" not in store

    assert hot_cache.get(key="also_allowed:foo_hot") == 1
    assert "also_allowed:foo_hot" in store  # is hot and allowed

    assert hot_cache.get(key="also_allowed:foo_cold") == 1
    assert "also_allowed:foo_cold" not in store

    assert hot_cache.get(key="also_allowed:foo_miss") is None
    assert "also_allowed:foo_miss" not in store

    assert metrics_collector.get_counters() == {
        "test_hot_cache_hits": 0,
        "test_hot_cache_misses": 6,
        "test_hot_cache_skips": 3,
        "test_hot_cache_item_count": 2,
        "test_hot_cache_hot_candidates": 2,
        "test_hot_cache_hot_skips": 1,
        "test_hot_cache_candidate_misses": 2,
    }


def test_multi_get_prefixes(
    time: Mock,
    client: Mock,
) -> None:
    store = {}
    metrics_collector = PrometheusMetricsCollector(
        namespace="test", registry=CollectorRegistry()
    )
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=["allowed:", "also_allowed:"],
        metrics_collector=metrics_collector,
    )

    time.time.return_value = 0

    assert hot_cache.multi_get(keys=["allowed:hot"]) == {Key(key="allowed:hot"): 1}
    assert hot_cache.multi_get(keys=["allowed:cold"]) == {Key(key="allowed:cold"): 1}
    assert hot_cache.multi_get(keys=["allowed:miss"]) == {Key(key="allowed:miss"): None}
    assert hot_cache.multi_get(keys=["hot"]) == {Key(key="hot"): 1}
    assert hot_cache.multi_get(keys=["cold"]) == {Key(key="cold"): 1}
    assert hot_cache.multi_get(keys=["miss"]) == {Key(key="miss"): None}
    assert "allowed:hot" in store
    assert "allowed:cold" not in store
    assert "allowed:miss" not in store
    assert "hot" not in store
    assert "cold" not in store
    assert "miss" not in store

    store.clear()

    assert hot_cache.multi_get(
        keys=["allowed:hot", "allowed:cold", "allowed:miss", "hot", "cold", "miss"]
    ) == {
        Key(key="allowed:hot"): 1,
        Key(key="allowed:cold"): 1,
        Key(key="allowed:miss"): None,
        Key(key="hot"): 1,
        Key(key="cold"): 1,
        Key(key="miss"): None,
    }
    assert "allowed:hot" in store
    assert "allowed:cold" not in store
    assert "allowed:miss" not in store
    assert "hot" not in store
    assert "cold" not in store
    assert "miss" not in store


def test_random_factor(
    time: Mock,
    random: Mock,
    client: Mock,
) -> None:
    store = {}
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=100,
        max_stale_while_revalidate_seconds=10,
    )

    time.time.return_value = 0

    # Even if key is found to be hot, only 1 out of probability_factor
    # will be stored in the hot cache.
    random.getrandbits.return_value = 1
    assert hot_cache.get(key="hot") == 1
    assert "hot" not in store

    # with the right random result, it gets stored
    random.getrandbits.return_value = 100
    assert hot_cache.get(key="hot") == 1
    assert "hot" in store


def test_multi_get(
    time: Mock,
    client: Mock,
) -> None:
    store = {}
    metrics_collector = PrometheusMetricsCollector(
        namespace="test", registry=CollectorRegistry()
    )
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=["allowed:"],
        metrics_collector=metrics_collector,
    )

    time.time.return_value = 0

    assert hot_cache.multi_get(
        keys=["allowed:hot", "allowed:cold", "allowed:miss", "hot", "cold", "miss"]
    ) == {
        Key(key="allowed:hot"): 1,
        Key(key="allowed:cold"): 1,
        Key(key="allowed:miss"): None,
        Key(key="hot"): 1,
        Key(key="cold"): 1,
        Key(key="miss"): None,
    }
    assert "allowed:hot" in store
    assert "allowed:cold" not in store
    assert "allowed:miss" not in store
    assert "hot" not in store
    assert "cold" not in store
    assert "miss" not in store
    client.meta_multiget.assert_called_once_with(
        keys=[
            Key("allowed:hot"),
            Key("allowed:cold"),
            Key("allowed:miss"),
            Key("hot"),
            Key("cold"),
            Key("miss"),
        ],
        **DEFAULT_FLAGS,
    )
    client.meta_multiget.reset_mock()

    # Second call produces the same result, but allowed:hot is returned
    # from the hot cache, not requested from the server
    assert hot_cache.multi_get(
        keys=["allowed:hot", "allowed:cold", "allowed:miss", "hot", "cold", "miss"]
    ) == {
        Key(key="allowed:hot"): 1,
        Key(key="allowed:cold"): 1,
        Key(key="allowed:miss"): None,
        Key(key="hot"): 1,
        Key(key="cold"): 1,
        Key(key="miss"): None,
    }
    assert "allowed:hot" in store
    assert "allowed:cold" not in store
    assert "allowed:miss" not in store
    assert "hot" not in store
    assert "cold" not in store
    assert "miss" not in store
    client.meta_multiget.assert_called_once_with(
        keys=[
            Key("allowed:cold"),
            Key("allowed:miss"),
            Key("hot"),
            Key("cold"),
            Key("miss"),
        ],
        **DEFAULT_FLAGS,
    )
    assert metrics_collector.get_counters() == {
        "test_hot_cache_hits": 1,
        "test_hot_cache_misses": 5,
        "test_hot_cache_skips": 6,
        "test_hot_cache_item_count": 1,
        "test_hot_cache_hot_candidates": 1,
        "test_hot_cache_hot_skips": 2,
        "test_hot_cache_candidate_misses": 2,
    }


def test_hot_miss_invalidates_hot_cache(
    time: Mock,
    client: Mock,
) -> None:
    store = {}
    metrics_collector = PrometheusMetricsCollector(
        namespace="test", registry=CollectorRegistry()
    )
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=None,
        metrics_collector=metrics_collector,
        immutable_types=[],
    )

    time.time.return_value = 0

    # Request a key that is hot, it is stored in the hot cache
    assert hot_cache.get(key="foo_hot") == 1
    assert hot_cache.multi_get(["multi_hot", "multi_cold"]) == {
        Key("multi_hot"): 1,
        Key("multi_cold"): 1,
    }
    assert hot_cache.multi_get(["one_hot", "two_hot"]) == {
        Key("one_hot"): 1,
        Key("two_hot"): 1,
    }
    assert store.get("foo_hot") == CachedValue(value=1, expiration=60, extended=False)
    assert store.get("multi_hot") == CachedValue(value=1, expiration=60, extended=False)
    assert store.get("multi_cold") is None
    assert store.get("one_hot") == CachedValue(value=1, expiration=60, extended=False)
    assert store.get("two_hot") == CachedValue(value=1, expiration=60, extended=False)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_multiget.assert_any_call(
        keys=[Key(key="multi_hot"), Key(key="multi_cold")], **DEFAULT_FLAGS
    )
    client.meta_multiget.assert_any_call(
        keys=[Key(key="one_hot"), Key(key="two_hot")], **DEFAULT_FLAGS
    )
    client.meta_get.reset_mock()
    client.meta_multiget.reset_mock()

    # Time goes by
    time.time.return_value = 60

    # mimic cache miss, so we can see the hot cache extended by the winner thread
    client.meta_get.side_effect = lambda *args, **kwargs: Miss()
    client.meta_multiget.side_effect = lambda keys, *args, **kwargs: {
        key: Miss() for key in keys
    }

    # First call gets the miss, extends the hot cache and returns None
    assert hot_cache.get(key="foo_hot") is None
    assert hot_cache.multi_get(["multi_hot", "multi_cold"]) == {
        Key("multi_hot"): None,
        Key("multi_cold"): None,
    }
    assert hot_cache.multi_get(["one_hot", "two_hot"]) == {
        Key("one_hot"): None,
        Key("two_hot"): None,
    }
    assert store.get("foo_hot") == CachedValue(value=1, expiration=70, extended=True)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_multiget.assert_any_call(
        keys=[Key(key="multi_hot"), Key(key="multi_cold")], **DEFAULT_FLAGS
    )
    client.meta_multiget.assert_any_call(
        keys=[Key(key="one_hot"), Key(key="two_hot")], **DEFAULT_FLAGS
    )
    client.meta_get.reset_mock()
    client.meta_multiget.reset_mock()

    # Second call gets the cached Value, that hasn't been cleared from hot cache
    assert hot_cache.get(key="foo_hot") == 1
    assert hot_cache.multi_get(["multi_hot", "multi_cold"]) == {
        Key("multi_hot"): 1,
        Key("multi_cold"): None,
    }
    assert hot_cache.multi_get(["one_hot", "two_hot"]) == {
        Key("one_hot"): 1,
        Key("two_hot"): 1,
    }
    assert store.get("foo_hot") == CachedValue(value=1, expiration=70, extended=True)
    assert store.get("multi_hot") == CachedValue(value=1, expiration=70, extended=True)
    assert store.get("multi_cold") is None
    assert store.get("one_hot") == CachedValue(value=1, expiration=70, extended=True)
    assert store.get("two_hot") == CachedValue(value=1, expiration=70, extended=True)
    client.meta_get.assert_not_called()
    client.meta_multiget.assert_called_once_with(
        keys=[Key(key="multi_cold")], **DEFAULT_FLAGS
    )
    client.meta_get.reset_mock()
    client.meta_multiget.reset_mock()

    # Time goes by
    time.time.return_value = 71

    # If the origin cache is still miss, the value will be cleared from hot cache
    assert hot_cache.get(key="foo_hot") is None
    assert hot_cache.multi_get(["multi_hot", "multi_cold"]) == {
        Key("multi_hot"): None,
        Key("multi_cold"): None,
    }
    assert hot_cache.multi_get(["one_hot", "two_hot"]) == {
        Key("one_hot"): None,
        Key("two_hot"): None,
    }
    assert store.get("foo_hot") is None
    assert store.get("multi_hot") is None
    assert store.get("multi_cold") is None
    assert store.get("one_hot") is None
    assert store.get("two_hot") is None
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_multiget.assert_any_call(
        keys=[Key(key="multi_hot"), Key(key="multi_cold")], **DEFAULT_FLAGS
    )
    client.meta_multiget.assert_any_call(
        keys=[Key(key="one_hot"), Key(key="two_hot")], **DEFAULT_FLAGS
    )
    client.meta_get.reset_mock()
    client.meta_multiget.reset_mock()

    assert metrics_collector.get_counters() == {
        "test_hot_cache_hits": 4,
        "test_hot_cache_misses": 16,
        "test_hot_cache_skips": 0,
        "test_hot_cache_item_count": 0,
        "test_hot_cache_hot_candidates": 4,
        "test_hot_cache_hot_skips": 0,
        "test_hot_cache_candidate_misses": 11,
    }


def test_stale_expires(
    time: Mock,
    client: Mock,
) -> None:
    store = {}
    metrics_collector = PrometheusMetricsCollector(
        namespace="test", registry=CollectorRegistry()
    )
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=None,
        metrics_collector=metrics_collector,
        immutable_types=[],
    )

    time.time.return_value = 0

    # Request a key that is hot, it is stored in the hot cache
    assert hot_cache.get(key="foo_hot") == 1
    assert store.get("foo_hot") == CachedValue(value=1, expiration=60, extended=False)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    # Time goes by
    time.time.return_value = 10
    assert hot_cache.get(key="foo_hot") == 1
    assert store.get("foo_hot") == CachedValue(value=1, expiration=60, extended=False)
    client.meta_get.assert_not_called()
    client.meta_get.reset_mock()

    # Time goes by so much that the item will expire
    time.time.return_value = 300
    # And the item is no longer hot
    client.meta_get.side_effect = lambda *args, **kwargs: Value(
        size=1,
        value=1,
        flags=ResponseFlags(
            fetched=True,
            last_access=9999,
        ),
    )

    # The item will no longer be in the hot cache
    assert hot_cache.get(key="foo_hot") == 1
    assert store.get("foo_hot") is None
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()


def test_cache_pollution_immutable_vs_mutable(
    time: Mock,
    client: Mock,
) -> None:
    """Test that immutable values aren't copied but mutable values are cloned
    to prevent cache pollution."""
    store = {}
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=None,
    )

    time.time.return_value = 0

    # Mock client to return different types of values
    def meta_get_with_types(
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> ReadResponse:
        if key.key == "immutable_int":
            return Value(
                size=1,
                value=42,
                flags=ResponseFlags(fetched=True, last_access=1),
            )
        elif key.key == "immutable_str":
            return Value(
                size=1,
                value="hello",
                flags=ResponseFlags(fetched=True, last_access=1),
            )
        elif key.key == "mutable_list":
            return Value(
                size=1,
                value=[1, 2, 3],
                flags=ResponseFlags(fetched=True, last_access=1),
            )
        elif key.key == "mutable_dict":
            return Value(
                size=1,
                value={"key": "value"},
                flags=ResponseFlags(fetched=True, last_access=1),
            )
        else:
            return Miss()

    client.meta_get.side_effect = meta_get_with_types

    # Test immutable integer - should not be copied
    result_int_1 = hot_cache.get(key="immutable_int")
    assert result_int_1 == 42
    assert "immutable_int" in store
    assert store["immutable_int"]._is_serialized is False

    # Second call should return the same value (no copying for immutables)
    result_int_2 = hot_cache.get(key="immutable_int")
    assert result_int_2 == 42
    assert result_int_2 is result_int_1  # Same object for immutable types

    # Test immutable string - should not be copied
    result_str_1 = hot_cache.get(key="immutable_str")
    assert result_str_1 == "hello"
    assert "immutable_str" in store
    assert store["immutable_str"]._is_serialized is False

    result_str_2 = hot_cache.get(key="immutable_str")
    assert result_str_2 == "hello"
    assert result_str_2 is result_str_1  # Same object for immutable types

    # Test mutable list - should be cloned on retrieval
    result_list_1 = hot_cache.get(key="mutable_list")
    assert result_list_1 == [1, 2, 3]
    assert "mutable_list" in store
    assert store["mutable_list"]._is_serialized is True  # Stored as pickle

    # Second call should return a NEW copy
    result_list_2 = hot_cache.get(key="mutable_list")
    assert result_list_2 == [1, 2, 3]
    assert result_list_2 is not result_list_1  # Different object!

    # Modify the returned list - should NOT affect cached value
    result_list_2.append(4)
    assert result_list_2 == [1, 2, 3, 4]

    # Get again - should still return original [1, 2, 3]
    result_list_3 = hot_cache.get(key="mutable_list")
    assert result_list_3 == [1, 2, 3]  # Not affected by modification
    assert result_list_3 is not result_list_2  # Yet another different object

    # Test mutable dict - should be cloned on retrieval
    result_dict_1 = hot_cache.get(key="mutable_dict")
    assert result_dict_1 == {"key": "value"}
    assert "mutable_dict" in store
    assert store["mutable_dict"]._is_serialized is True  # Stored as pickle

    # Second call should return a NEW copy
    result_dict_2 = hot_cache.get(key="mutable_dict")
    assert result_dict_2 == {"key": "value"}
    assert result_dict_2 is not result_dict_1  # Different object!

    # Modify the returned dict - should NOT affect cached value
    result_dict_2["new_key"] = "new_value"
    assert result_dict_2 == {"key": "value", "new_key": "new_value"}

    # Get again - should still return original dict
    result_dict_3 = hot_cache.get(key="mutable_dict")
    assert result_dict_3 == {"key": "value"}  # Not affected by modification
    assert result_dict_3 is not result_dict_2  # Yet another different object


def test_cache_pollution_multi_get(
    time: Mock,
    client: Mock,
) -> None:
    """Test that multi_get also properly clones mutable values."""
    store = {}
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=None,
    )

    time.time.return_value = 0

    # Mock client to return mutable values
    def meta_multiget_with_types(
        keys: List[Key],
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> Dict[Key, ReadResponse]:
        results = {}
        for key in keys:
            if key.key == "list_hot":
                results[key] = Value(
                    size=1,
                    value=[10, 20, 30],
                    flags=ResponseFlags(fetched=True, last_access=1),
                )
            elif key.key == "dict_hot":
                results[key] = Value(
                    size=1,
                    value={"a": 1, "b": 2},
                    flags=ResponseFlags(fetched=True, last_access=1),
                )
        return results

    client.meta_multiget.side_effect = meta_multiget_with_types

    # First call - caches the values
    results_1 = hot_cache.multi_get(keys=["list_hot", "dict_hot"])
    assert results_1[Key("list_hot")] == [10, 20, 30]
    assert results_1[Key("dict_hot")] == {"a": 1, "b": 2}
    assert "list_hot" in store
    assert "dict_hot" in store

    # Second call - should return cloned copies
    results_2 = hot_cache.multi_get(keys=["list_hot", "dict_hot"])
    assert results_2[Key("list_hot")] == [10, 20, 30]
    assert results_2[Key("dict_hot")] == {"a": 1, "b": 2}

    # Verify they are different objects
    assert results_2[Key("list_hot")] is not results_1[Key("list_hot")]
    assert results_2[Key("dict_hot")] is not results_1[Key("dict_hot")]

    # Modify the returned values
    results_2[Key("list_hot")].append(40)
    results_2[Key("dict_hot")]["c"] = 3

    # Get again - should return unmodified values
    results_3 = hot_cache.multi_get(keys=["list_hot", "dict_hot"])
    assert results_3[Key("list_hot")] == [10, 20, 30]  # Not [10, 20, 30, 40]
    assert results_3[Key("dict_hot")] == {
        "a": 1,
        "b": 2,
    }  # Not {"a": 1, "b": 2, "c": 3}
