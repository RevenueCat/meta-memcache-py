from typing import Dict, List, Optional, Set
from unittest.mock import Mock

import pytest

from meta_memcache import CacheClient, IntFlag, Key, Value
from meta_memcache.errors import MemcacheError
from meta_memcache.extras.probabilistic_hot_cache import (
    CachedValue,
    ProbabilisticHotCache,
)
from meta_memcache.protocol import Flag, Miss, ReadResponse, TokenFlag


@pytest.fixture
def client() -> Mock:
    def meta_get(
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> ReadResponse:
        if key.key.endswith("hot"):
            return Value(
                size=1,
                value=1,
                int_flags={
                    IntFlag.HIT_AFTER_WRITE: 1,
                    IntFlag.LAST_READ_AGE: 1,
                },
            )
        elif key.key.endswith("miss"):
            return Miss()
        else:
            return Value(
                size=1,
                value=1,
                int_flags={
                    IntFlag.HIT_AFTER_WRITE: 1,
                    IntFlag.LAST_READ_AGE: 9999,
                },
            )

    def meta_multiget(
        keys: List[Key],
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
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
    "flags": {
        Flag.RETURN_TTL,
        Flag.RETURN_LAST_ACCESS,
        Flag.RETURN_VALUE,
        Flag.RETURN_FETCHED,
        Flag.RETURN_CLIENT_FLAG,
    },
    "int_flags": {},
    "token_flags": None,
}


def test_get_without_prefixes(
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
    originsl_meta_get = client.meta_get.side_effect
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
    client.meta_get.side_effect = originsl_meta_get

    assert hot_cache.get(key="foo_hot") == 1
    assert store.get("foo_hot") == CachedValue(value=1, expiration=190, extended=False)
    client.meta_get.assert_called_once_with(key=Key(key="foo_hot"), **DEFAULT_FLAGS)
    client.meta_get.reset_mock()

    assert hot_cache.get(key="foo_hot") == 1
    client.meta_get.assert_not_called()


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
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=["allowed:", "also_allowed:"],
    )

    time.time.return_value = 0

    # Request a key that is a miss. It will not be cached
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
    hot_cache = ProbabilisticHotCache(
        client=client,
        store=store,
        cache_ttl=60,
        max_last_access_age_seconds=10,
        probability_factor=1,
        max_stale_while_revalidate_seconds=10,
        allowed_prefixes=["allowed:"],
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
