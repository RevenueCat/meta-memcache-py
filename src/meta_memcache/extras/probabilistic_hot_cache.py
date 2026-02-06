import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import pickle

from marisa_trie import Trie  # type: ignore

from meta_memcache.configuration import RecachePolicy
from meta_memcache.extras.client_wrapper import ClientWrapper
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.metrics.base import BaseMetricsCollector, MetricDefinition
from meta_memcache.protocol import Key, Value

IMMUTABLE_TYPES = frozenset((type(None), bool, int, float, str, bytes))

# Use this if you know when you store tuples / frozensets in cache, they don't contain
# mutable objects, ie, not list, dict, set, etc on their values.
IMMUTABLE_TYPES_RISKY = frozenset(
    (type(None), bool, int, float, str, bytes, tuple, frozenset)
)


@dataclass(slots=True)
class CachedValue:
    _value: Any
    expiration: int
    extended: bool = False
    _is_serialized: bool = False

    def __init__(
        self, value: Any, expiration: int, extended: bool, is_immutable: bool = False
    ):
        self._value = (
            value
            if is_immutable
            else pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        )
        self.expiration = expiration
        self.extended = extended
        self._is_serialized = not is_immutable

    def get_cloned_value(self) -> Any:
        return pickle.loads(self._value) if self._is_serialized else self._value


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
        metrics_collector: Optional[BaseMetricsCollector] = None,
        immutable_types: Iterable[type] = IMMUTABLE_TYPES,
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
        if metrics_collector:
            metrics_collector.init_metrics(
                namespace="hot_cache",
                metrics=[
                    MetricDefinition("hits", "Number of hits"),
                    MetricDefinition("misses", "Number of misses"),
                    MetricDefinition(
                        "skips", "Number of skipped keys (not in allowed prefixes)"
                    ),
                    MetricDefinition(
                        "hot_skips", "Keys detected hot but not in allowed prefixes"
                    ),
                    MetricDefinition(
                        "hot_candidates",
                        "Keys detected hot and candidates to be cached",
                    ),
                    MetricDefinition(
                        "candidate_misses",
                        "Number of misses for keys in allowed prefixes",
                    ),
                ],
                gauges=[
                    MetricDefinition("item_count", "Number of items in the cache"),
                ],
            )
        self._metrics = metrics_collector
        self._immutable_types = immutable_types

    def _lookup_hot_cache(
        self,
        key: Key,
    ) -> Tuple[bool, bool, Optional[Any]]:
        is_found: bool
        is_hot: bool
        value: Optional[Any]
        if found := self._store.get(key.key):
            is_hot = True
            now = int(time.time())
            ttl = found.expiration - now
            if ttl > 0:
                is_found = True
            elif (
                not found.extended
                and abs(ttl) < self._max_stale_while_revalidate_seconds
            ):
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
                # Expired and the value is too stale to use. No longer hot.
                self._clear_hot_cache_if_necessary(key)
                is_found = False
                is_hot = False
            value = found.get_cloned_value() if is_found else None
        else:
            # Not found so not hot
            is_found = False
            is_hot = False
            value = None

        self._metrics and self._metrics.metric_inc("hits" if is_found else "misses")
        return is_found, is_hot, value

    def _store_in_hot_cache_if_necessary(
        self,
        key: Key,
        value: Value,
        is_hot: bool,
        allowed: bool,
    ) -> None:
        if not is_hot:
            last_read_age = (
                value.flags.last_access if value.flags.last_access is not None else 9999
            )
            if (
                value.flags.fetched
                and last_read_age <= self._max_last_access_age_seconds
            ):
                # Is detected as hot
                if allowed:
                    self._metrics and self._metrics.metric_inc("hot_candidates")
                    is_hot = random.getrandbits(10) % self._probability_factor == 0
                else:
                    self._metrics and self._metrics.metric_inc("hot_skips")
        if not is_hot:
            return

        is_immutable = type(value.value) in self._immutable_types
        self._store[key.key] = CachedValue(
            value=value.value,
            expiration=int(time.time()) + self._cache_ttl,
            extended=False,
            is_immutable=is_immutable,
        )
        self._metrics and self._metrics.gauge_set("item_count", len(self._store))

    def _clear_hot_cache_if_necessary(self, key: Key) -> bool:
        if found := self._store.get(key.key):
            if time.time() > found.expiration:
                del self._store[key.key]
                self._metrics and self._metrics.gauge_set(
                    "item_count", len(self._store)
                )
                return True
        return False

    def get(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        key = key if isinstance(key, Key) else Key(key)
        if self._allowed_prefixes and not self._allowed_prefixes.prefixes(key.key):
            is_hot = False
            allowed = False
            self._metrics and self._metrics.metric_inc("skips")
        else:
            allowed = True
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
            allowed and self._metrics and self._metrics.metric_inc("candidate_misses")
            is_hot and self._clear_hot_cache_if_necessary(key)
            return None
        else:
            self._store_in_hot_cache_if_necessary(key, result, is_hot, allowed)
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
            if self._metrics and ineligible_keys:
                self._metrics.metric_inc("skips", len(ineligible_keys))
            results = self._multi_get(
                keys=pending_keys + ineligible_keys,
                touch_ttl=touch_ttl,
                recache_policy=recache_policy,
            )
            for key, result in results.items():
                allowed = key not in ineligible_keys
                is_hot = key in values if allowed else False
                if result is None:
                    allowed and self._metrics and self._metrics.metric_inc(
                        "candidate_misses"
                    )
                    is_hot and self._clear_hot_cache_if_necessary(key)
                    values[key] = None
                else:
                    self._store_in_hot_cache_if_necessary(key, result, is_hot, allowed)
                    values[key] = result.value
        return values
