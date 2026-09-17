"""Tests specific to the in-memory ProbabilisticHotCache.

The behaviour it shares with the sqlite implementation is covered in
probabilistic_hot_cache_conformance_test.py. What is left here is what
only the in-memory store does: keeping values by reference when they are
immutable, and the bookkeeping of the CachedValue entries in the dict.
"""

from typing import Dict, List, Optional
from unittest.mock import Mock

import pytest
from prometheus_client import CollectorRegistry

from meta_memcache import Key, Value
from meta_memcache.extras.probabilistic_hot_cache import (
    CachedValue,
    ProbabilisticHotCache,
)
from meta_memcache.interfaces.router import DEFAULT_FAILURE_HANDLING, FailureHandling
from meta_memcache.metrics.prometheus import PrometheusMetricsCollector
from meta_memcache.protocol import Miss, ReadResponse, RequestFlags, ResponseFlags
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
    monkeypatch.setattr("meta_memcache.extras.probabilistic_hot_cache.time", time_mock)
    return time_mock


def build_cache(
    client: Mock, store: Dict[str, CachedValue], **overrides
) -> ProbabilisticHotCache:
    return ProbabilisticHotCache(
        client=client, store=store, **{**DEFAULT_SETTINGS, **overrides}
    )


def test_entry_clocks_through_the_stale_cycle(client: Mock, time: Mock) -> None:
    """The two clocks of a CachedValue, over the life of a hot value."""
    store: Dict[str, CachedValue] = {}
    hot_cache = build_cache(client, store, immutable_types=[])

    # A fresh value is revalidated as soon as it goes stale
    assert hot_cache.get("foo_hot") == 1
    assert store["foo_hot"] == CachedValue(value=1, expiration=60, revalidate_at=60)

    # Serving it does not touch the clocks
    time.time.return_value = 30
    assert hot_cache.get("foo_hot") == 1
    assert store["foo_hot"] == CachedValue(value=1, expiration=60, revalidate_at=60)

    # The elected thread pushes the retry clock forward and leaves the
    # expiration, and so the hard deadline, alone
    time.time.return_value = 61
    assert hot_cache._lookup_hot_cache(Key("foo_hot")) == revalidating(1)
    assert store["foo_hot"] == CachedValue(value=1, expiration=60, revalidate_at=62)

    # The threads served the stale value meanwhile do not touch them either
    assert hot_cache._lookup_hot_cache(Key("foo_hot")) == hot(1)
    assert store["foo_hot"] == CachedValue(value=1, expiration=60, revalidate_at=62)

    # Nobody refreshed it, so it is dropped at the hard deadline (60 + 10)
    time.time.return_value = 70
    assert hot_cache._lookup_hot_cache(Key("foo_hot")) is None
    assert "foo_hot" not in store

    # And a fresh read starts the cycle over
    assert hot_cache.get("foo_hot") == 1
    assert store["foo_hot"] == CachedValue(value=1, expiration=130, revalidate_at=130)


def test_item_count_gauge_tracks_the_store(client: Mock, time: Mock) -> None:
    store: Dict[str, CachedValue] = {}
    metrics = PrometheusMetricsCollector(namespace="test", registry=CollectorRegistry())
    hot_cache = build_cache(client, store, metrics_collector=metrics)

    def item_count() -> int:
        return metrics.get_counters()["test_hot_cache_item_count"]

    assert hot_cache.get("foo_hot") == 1
    assert hot_cache.get("bar_hot") == 1
    assert item_count() == 2

    # The key is gone from the server: the entry is dropped and counted out
    client.meta_get.side_effect = lambda key, **kwargs: Miss()
    time.time.return_value = 61
    assert hot_cache.get("foo_hot") is None
    assert item_count() == 1


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
