import pickle
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from marisa_trie import Trie  # type: ignore

from meta_memcache.configuration import LeasePolicy, RecachePolicy
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
    """
    A hot value, with the two clocks that govern its lifetime.

    `expiration` is when the value goes stale, and is never mutated after
    the store, so it is also the hard deadline: the value must not be served
    past expiration + max_stale_while_revalidate_seconds. `revalidate_at` is
    the separate retry clock the threads race on to elect a single
    revalidator, and defaults to the expiration: a fresh value is
    revalidated as soon as it goes stale.
    """

    _value: Any
    expiration: int
    revalidate_at: int
    _is_serialized: bool = False

    def __init__(
        self,
        value: Any,
        expiration: int,
        revalidate_at: Optional[int] = None,
        is_immutable: bool = False,
    ):
        self._value = (
            value
            if is_immutable
            else pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        )
        self.expiration = expiration
        self.revalidate_at = expiration if revalidate_at is None else revalidate_at
        self._is_serialized = not is_immutable

    def get_cloned_value(self) -> Any:
        return pickle.loads(self._value) if self._is_serialized else self._value


class ProbabilisticHotCache(ClientWrapper):
    """
    Caches locally the values detected as hot, to offload the cache server.

    Keys read often enough (according to the server's last access time) are
    promoted to the local store with probability 1/probability_factor, and
    served from there for cache_ttl seconds.

    Once a value goes stale, a single thread is elected to revalidate it
    while the rest keep being served the stale value, to avoid thundering
    herds. If the elected thread fails to refresh it, another one is elected
    revalidation_retry_seconds later.

    Stale values are only served within a bounded grace window: a value is
    dropped once it is more than max_stale_while_revalidate_seconds past its
    expiration, regardless of how often it is read. If the server keeps
    failing to revalidate it, the value expires rather than being served
    stale forever, and the key has to be detected as hot again.
    """

    # Subclasses extend these with the metrics of their own storage.
    _METRICS: Tuple[MetricDefinition, ...] = (
        MetricDefinition("hits", "Number of hits"),
        MetricDefinition("misses", "Number of misses"),
        MetricDefinition("skips", "Number of skipped keys (not in allowed prefixes)"),
        MetricDefinition("hot_skips", "Keys detected hot but not in allowed prefixes"),
        MetricDefinition(
            "hot_candidates", "Keys detected hot and candidates to be cached"
        ),
        MetricDefinition(
            "candidate_misses", "Number of misses for keys in allowed prefixes"
        ),
    )
    _GAUGES: Tuple[MetricDefinition, ...] = (
        MetricDefinition("item_count", "Number of items in the cache"),
    )

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
        revalidation_retry_seconds: int = 1,
    ) -> None:
        if revalidation_retry_seconds < 1:
            # The winner of the election must move revalidate_at strictly
            # forward, or every thread racing at the same second wins.
            raise ValueError("revalidation_retry_seconds must be at least 1")
        super().__init__(client=client)
        self._store = store
        self._lock = threading.Lock()
        self._cache_ttl = cache_ttl
        self._max_last_access_age_seconds = max_last_access_age_seconds
        self._probability_factor = probability_factor
        self._max_stale_while_revalidate_seconds = max_stale_while_revalidate_seconds
        self._revalidation_retry_seconds = revalidation_retry_seconds
        self._allowed_prefixes: Optional[Trie] = (
            Trie(allowed_prefixes) if allowed_prefixes else None
        )
        if metrics_collector:
            metrics_collector.init_metrics(
                namespace="hot_cache",
                metrics=list(self._METRICS),
                gauges=list(self._GAUGES),
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
            if now < found.expiration:
                is_found = True
            elif now < found.expiration + self._max_stale_while_revalidate_seconds:
                # Expired, but within the grace window: the value is still
                # fresh enough to use stale-while-revalidate and avoid
                # thundering herds. Only one thread gets to refresh the cache,
                # by pushing the retry clock forward and mimicking a cache
                # miss, while the rest serve the stale value. If that thread
                # fails to refresh it, another one is elected once the retry
                # clock arrives, until the grace window runs out.
                is_found = True
                if now >= found.revalidate_at:
                    with self._lock:
                        # Check again in case another thread won the election
                        # while we were waiting for the lock.
                        if now >= found.revalidate_at:
                            found.revalidate_at = now + self._revalidation_retry_seconds
                            is_found = False
            else:
                # Past the grace window: nobody managed to revalidate it, so
                # the value is too stale to serve. Drop it and treat the key
                # as cold, it has to be detected as hot again.
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

        self._store_entry(key, value.value)

    def _store_entry(self, key: Key, value: Any) -> None:
        is_immutable = type(value) in self._immutable_types
        self._store[key.key] = CachedValue(
            value=value,
            expiration=int(time.time()) + self._cache_ttl,
            is_immutable=is_immutable,
        )
        self._metrics and self._metrics.gauge_set("item_count", len(self._store))

    def _clear_hot_cache_if_necessary(self, key: Key) -> bool:
        # Called when the server missed, and when an entry runs out of grace
        # window: drop the stale entry. Since expiration is never mutated
        # after the store, the guard is exact: a fresh value stored by
        # another thread in the meantime is preserved.
        if found := self._store.get(key.key):
            if found.expiration <= int(time.time()):
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
        return self._hot_cache_get(
            key=key,
            touch_ttl=touch_ttl,
            recache_policy=recache_policy,
        )

    def get_or_lease(
        self,
        key: Union[Key, str],
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        lease_wait_fn: Optional[Callable[[float], None]] = None,
    ) -> Optional[Any]:
        # A hot cache hit needs no lease: the value is already local and there
        # is nothing to repopulate. Only the requests that fall through to the
        # server take part in the lease.
        return self._hot_cache_get(
            key=key,
            touch_ttl=touch_ttl,
            recache_policy=recache_policy,
            lease_policy=lease_policy,
            lease_wait_fn=lease_wait_fn,
        )

    def _hot_cache_get(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        lease_policy: Optional[LeasePolicy] = None,
        lease_wait_fn: Optional[Callable[[float], None]] = None,
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

        if lease_policy is not None:
            result = self._get_or_lease(
                key=key,
                lease_policy=lease_policy,
                touch_ttl=touch_ttl,
                recache_policy=recache_policy,
                lease_wait_fn=lease_wait_fn,
            )
            if result and result.value is None:
                # Lease placeholder: we hold the lease, or we lost and ran out
                # of retries. Behaves as a miss, and nothing worth promoting.
                result = None
        else:
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
