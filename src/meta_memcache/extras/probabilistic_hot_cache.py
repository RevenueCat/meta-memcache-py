import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from marisa_trie import Trie  # type: ignore

from meta_memcache.configuration import RecachePolicy
from meta_memcache.extras.client_wrapper import ClientWrapper
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.protocol import IntFlag, Key, Value


@dataclass
class CachedValue:
    value: Any
    expiration: int
    extended: bool


class ProbabilisticHotCache(ClientWrapper):
    def __init__(
        self,
        client: CacheApi,
        store: Dict[str, CachedValue],
        cache_ttl: int,
        max_last_access_age_seconds: int,
        probability_factor: int,
        max_stale_while_revalidate_seconds: int = 10,
        allowed_prefixes: Optional[List[str]] = None,
    ) -> None:
        super().__init__(client=client)
        self._store = store
        self._lock = threading.Lock()
        self._cache_ttl = cache_ttl
        self._max_last_access_age_seconds = max_last_access_age_seconds
        self._probability_factor = probability_factor
        self._max_stale_while_revalidate_seconds = max_stale_while_revalidate_seconds
        self._allowed_prefixes: Optional[Trie] = (
            Trie(allowed_prefixes) if allowed_prefixes else None
        )

    def _lookup_hot_cache(
        self,
        key: Key,
    ) -> Tuple[bool, bool, Optional[Any]]:
        is_found: bool
        is_hot: bool
        value: Optional[Any]
        if found := self._store.get(key.key):
            is_hot = True
            value = found.value
            now = int(time.time())
            ttl = found.expiration - now
            if ttl > 0:
                is_found = True
            elif not found.extended and ttl < self._max_stale_while_revalidate_seconds:
                # Expired but the value is still fresh enough. We will try to
                # use stale-while-revalidate to avoid thundering herds. Only
                # one thread will get to refresh the cache.
                is_found = True
                with self._lock:
                    # Check again in case another thread refreshed the cache
                    # while we were waiting for the lock.
                    if not found.extended:
                        # We get to refresh the cache.
                        # We do this by extending the expiration time so other
                        # threads will use the stale value while we refresh.
                        # and mimicking a cache miss.
                        found.expiration += self._max_stale_while_revalidate_seconds
                        found.extended = True
                        is_found = False
            else:
                # Expired and the value is too stale to use.
                is_found = False
        else:
            # Not found so not hot
            is_found = False
            is_hot = False
            value = None

        return is_found, is_hot, value

    def _store_in_hot_cache_if_necessary(
        self,
        key: Key,
        value: Value,
        is_hot: bool,
    ) -> None:
        if not is_hot:
            hit_after_write = value.int_flags.get(IntFlag.HIT_AFTER_WRITE, 0)
            last_read_age = value.int_flags.get(IntFlag.LAST_READ_AGE, 9999)
            is_hot = (
                hit_after_write > 0
                and last_read_age <= self._max_last_access_age_seconds
                and random.getrandbits(10) % self._probability_factor == 0
            )
        if not is_hot:
            return

        self._store[key.key] = CachedValue(
            value=value.value,
            expiration=int(time.time()) + self._cache_ttl,
            extended=False,
        )

    def get(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        key = key if isinstance(key, Key) else Key(key)
        if self._allowed_prefixes and not self._allowed_prefixes.prefixes(key.key):
            return super().get(
                key=key, touch_ttl=touch_ttl, recache_policy=recache_policy
            )
        found, is_hot, value = self._lookup_hot_cache(key=key)
        if found:
            return value

        result = self._get(
            key=key,
            touch_ttl=touch_ttl,
            recache_policy=recache_policy,
            return_cas_token=False,
        )
        if result is None:
            return None
        else:
            self._store_in_hot_cache_if_necessary(key, result, is_hot)
            return result.value

    def multi_get(
        self,
        keys: Iterable[Union[Key, str]],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Dict[Key, Optional[Any]]:
        _keys: List[Key] = [key if isinstance(key, Key) else Key(key) for key in keys]
        values: Dict[Key, Optional[Any]] = {}
        pending_keys: List[Key] = []
        ineligible_keys: List[Key] = []
        for key in _keys:
            if self._allowed_prefixes and not self._allowed_prefixes.prefixes(key.key):
                ineligible_keys.append(key)
                continue
            found, is_hot, value = self._lookup_hot_cache(key=key)
            if is_hot:
                values[key] = value
            if not found:
                pending_keys.append(key)

        if pending_keys or ineligible_keys:
            results = self._multi_get(
                keys=pending_keys + ineligible_keys,
                touch_ttl=touch_ttl,
                recache_policy=recache_policy,
            )
            for key, result in results.items():
                if result is None:
                    values[key] = None
                else:
                    if key not in ineligible_keys:
                        is_hot = key in values
                        self._store_in_hot_cache_if_necessary(key, result, is_hot)
                    values[key] = result.value
        return values