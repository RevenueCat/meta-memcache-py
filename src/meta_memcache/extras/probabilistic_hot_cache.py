import pickle
import random
import threading
import time
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

from marisa_trie import Trie  # type: ignore

from meta_memcache.commands.high_level_commands import DEFAULT_GET_FLAGS
from meta_memcache.configuration import LeasePolicy, RecachePolicy
from meta_memcache.errors import MemcacheError
from meta_memcache.extras.client_wrapper import ClientWrapper
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.metrics.base import BaseMetricsCollector, MetricDefinition
from meta_memcache.protocol import Key, RequestFlags, Value, is_error_response


class HotCacheLookup(NamedTuple):
    """
    What the hot cache holds for a key, and what the caller has to do.

    A lookup returns None when the key is not hot, so there is no state
    here for "nothing found": `value` is always something we hold, fresh or
    stale. It is carried even when the caller must revalidate, so a
    revalidation that fails can fall back on it without going back to the
    store for it.
    """

    value: Any
    must_revalidate: bool


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

    With extend_on_error, a hot value is not dropped when the server fails
    to answer: the request that hit the error is served the stale value, and
    the value is stored again as if the revalidation had succeeded, which
    buys it another cache_ttl. Since only a failure does this, hot values
    last as long as the outage does and go back to expiring normally as soon
    as the server answers again. That keeps the traffic off whatever sits
    behind the cache, at the cost of serving values that may be arbitrarily
    stale, so it is off by default. A miss from a server that answered is
    still a miss, and still drops the value.

    Keys in hot_keys are known to be hot: they are promoted on their first
    read, regardless of the server's last access signal and the probability
    factor, but still only under allowed_prefixes. That signal is unreliable
    under memcached's segmented LRU, where the last access time tracks LRU
    activations rather than reads. Replace the list at any time with
    set_hot_keys().
    """

    # Subclasses extend these with the metrics of their own storage.
    _METRICS: Tuple[MetricDefinition, ...] = (
        MetricDefinition("hits", "Number of hits"),
        MetricDefinition("misses", "Number of misses"),
        MetricDefinition("skips", "Number of skipped keys (not in allowed prefixes)"),
        MetricDefinition(
            "hot_skips", "Keys known or detected hot but not in allowed prefixes"
        ),
        MetricDefinition(
            "hot_candidates", "Keys detected hot and candidates to be cached"
        ),
        MetricDefinition(
            "listed_promotions", "Keys promoted because they are in the hot key list"
        ),
        MetricDefinition(
            "candidate_misses", "Number of misses for keys in allowed prefixes"
        ),
        MetricDefinition(
            "error_extensions",
            "Hot values kept alive and served stale because the server failed",
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
        extend_on_error: bool = False,
        hot_keys: Iterable[str] = (),
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
        self._extend_on_error = extend_on_error
        self._hot_keys: FrozenSet[str] = frozenset()
        self.set_hot_keys(hot_keys)

    def set_hot_keys(self, keys: Iterable[str]) -> None:
        """Replace the hot key list. Atomic: readers see the old set or the new one."""
        if isinstance(keys, (str, bytes)):
            # frozenset("abc") is {"a", "b", "c"}
            raise TypeError("hot_keys must be a collection of keys, not a str")
        self._hot_keys = frozenset(keys)

    def _read_flags(
        self,
        touch_ttl: Optional[int],
        recache_policy: Optional[RecachePolicy],
    ) -> RequestFlags:
        # The hot cache reads through meta_get()/meta_multiget() rather than
        # _get()/_multi_get(), because those turn both a miss and a failed
        # request into None, and telling them apart is the whole point.
        return DEFAULT_GET_FLAGS.replace(
            recache_ttl=recache_policy.ttl if recache_policy else None,
            cache_ttl=touch_ttl if touch_ttl is not None and touch_ttl >= 0 else None,
        )

    def _lookup_hot_cache(self, key: Key) -> Optional[HotCacheLookup]:
        found = self._store.get(key.key)
        if found is None:
            return self._miss()

        now = int(time.time())
        if now >= found.expiration + self._max_stale_while_revalidate_seconds:
            # Past the grace window: nobody managed to revalidate it, so the
            # value is too stale to serve. Drop it and treat the key as cold,
            # it has to be detected as hot again.
            self._clear_hot_cache_if_necessary(key)
            return self._miss()

        must_revalidate = False
        if now >= found.expiration and now >= found.revalidate_at:
            # Stale, and nobody is refreshing it: use stale-while-revalidate
            # to avoid thundering herds. Only one thread gets elected, by
            # pushing the retry clock forward, while the rest serve the stale
            # value. If the elected one fails to refresh it, another is
            # elected once the retry clock arrives, until the grace window
            # runs out.
            with self._lock:
                # Check again in case another thread won the election while
                # we were waiting for the lock.
                if now >= found.revalidate_at:
                    found.revalidate_at = now + self._revalidation_retry_seconds
                    must_revalidate = True

        return self._hit(found.get_cloned_value(), must_revalidate)

    def _miss(self) -> Optional[HotCacheLookup]:
        """Count a miss and report that we hold nothing for the key."""
        self._metrics and self._metrics.metric_inc("misses")
        return None

    def _hit(self, value: Any, must_revalidate: bool) -> HotCacheLookup:
        # The thread elected to revalidate counts as a miss: it is about to
        # go to the server, the same as if we had held nothing.
        self._metrics and self._metrics.metric_inc(
            "misses" if must_revalidate else "hits"
        )
        return HotCacheLookup(value=value, must_revalidate=must_revalidate)

    def _store_in_hot_cache_if_necessary(
        self,
        key: Key,
        value: Value,
        is_hot: bool,
        allowed: bool,
    ) -> None:
        if not is_hot:
            listed = key.key in self._hot_keys
            last_read_age = (
                value.flags.last_access if value.flags.last_access is not None else 9999
            )
            detected = (
                value.flags.fetched
                and last_read_age <= self._max_last_access_age_seconds
            )
            if listed or detected:
                if not allowed:
                    self._metrics and self._metrics.metric_inc("hot_skips")
                elif listed:
                    # Known hot: no server signal, no coin.
                    self._metrics and self._metrics.metric_inc("listed_promotions")
                    is_hot = True
                else:
                    self._metrics and self._metrics.metric_inc("hot_candidates")
                    is_hot = random.getrandbits(10) % self._probability_factor == 0
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

    def _serve_stale_on_error(self, key: Key, value: Any) -> Any:
        """
        Fall back on the value the lookup handed us, since the server failed.

        Storing it again is exactly what a successful revalidation does, so
        the entry gets a fresh cache_ttl and goes back to expiring normally
        as soon as the server answers. Only a failure does this, so hot
        values last as long as the outage lasts and no longer. The counter
        is what tells the two apart from the outside.
        """
        self._store_entry(key, value)
        self._metrics and self._metrics.metric_inc("error_extensions")
        return value

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
        found: Optional[HotCacheLookup] = None
        if self._allowed_prefixes and not self._allowed_prefixes.prefixes(key.key):
            allowed = False
            self._metrics and self._metrics.metric_inc("skips")
        else:
            allowed = True
            found = self._lookup_hot_cache(key=key)
            if found is not None and not found.must_revalidate:
                return found.value

        # A server failure reaches us either as an exception or, on a pool
        # that does not raise, as a miss flagged as an error. Neither means
        # the key is gone, so neither should cost us the value we hold. With
        # nothing held there is nothing to decide, and the failure is the
        # caller's to handle.
        rescue = found if self._extend_on_error else None
        try:
            if lease_policy is not None:
                result = self._get_or_lease(
                    key=key,
                    lease_policy=lease_policy,
                    touch_ttl=touch_ttl,
                    recache_policy=recache_policy,
                    lease_wait_fn=lease_wait_fn,
                )
                if result and result.value is None:
                    # Lease placeholder: we hold the lease, or we lost and ran
                    # out of retries. Behaves as a miss, and nothing worth
                    # promoting.
                    result = None
            else:
                response = self.meta_get(
                    key, flags=self._read_flags(touch_ttl, recache_policy)
                )
                if rescue is not None and is_error_response(response):
                    return self._serve_stale_on_error(key, rescue.value)
                result = self._process_get_result(key, response)
        except MemcacheError:
            if rescue is None:
                raise
            return self._serve_stale_on_error(key, rescue.value)

        is_hot = found is not None
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
            found = self._lookup_hot_cache(key=key)
            if found is None:
                pending_keys.append(key)
                continue
            # Held even when it has to be revalidated, so a revalidation
            # that fails can fall back on it.
            values[key] = found.value
            if found.must_revalidate:
                pending_keys.append(key)

        if pending_keys or ineligible_keys:
            if self._metrics and ineligible_keys:
                self._metrics.metric_inc("skips", len(ineligible_keys))
            try:
                responses = self.meta_multiget(
                    keys=pending_keys + ineligible_keys,
                    flags=self._read_flags(touch_ttl, recache_policy),
                )
            except MemcacheError:
                # The whole batch failed, which is what a pool that raises
                # does when any of its servers is down. Keep the hot values
                # we hold and let the rest behave as misses: raising would
                # throw away the ones we just rescued.
                if not (self._extend_on_error and values):
                    raise
                for key in pending_keys:
                    # Only the ones we were refreshing: a hot value that was
                    # still fresh never reached the failing request.
                    if key in values:
                        values[key] = self._serve_stale_on_error(key, values[key])
                return {key: values.get(key) for key in _keys}

            for key, response in responses.items():
                allowed = key not in ineligible_keys
                is_hot = key in values if allowed else False
                if is_error_response(response):
                    # Only the servers holding these keys failed; the rest of
                    # the batch answered normally. Nothing here says the key
                    # is gone, so the hot value stays.
                    values[key] = (
                        self._serve_stale_on_error(key, values[key])
                        if is_hot and self._extend_on_error
                        else None
                    )
                    continue
                result = self._process_get_result(key, response)
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
