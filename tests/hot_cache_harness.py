"""Shared fixtures for the ProbabilisticHotCache test suites.

The in-memory and the sqlite hot caches must behave identically as far as
callers are concerned, so the behaviour they share is tested once, in
probabilistic_hot_cache_conformance_test.py, against both. This module
holds the client mocks and the harness that builds either implementation
over a single backing store.
"""

import sqlite3
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import Mock

from meta_memcache import CacheClient, Key, Value
from meta_memcache.extras.probabilistic_hot_cache import (
    CachedValue,
    HotCacheLookup,
    ProbabilisticHotCache,
)
from meta_memcache.extras.probabilistic_hot_cache_sqlite import (
    HotCacheDBConfig,
    SqliteProbabilisticHotCache,
)
from meta_memcache.interfaces.router import DEFAULT_FAILURE_HANDLING, FailureHandling
from meta_memcache.metrics.base import BaseMetricsCollector
from meta_memcache.protocol import Miss, ReadResponse, RequestFlags, ResponseFlags

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

LEASE_FLAGS = {
    "flags": RequestFlags(
        return_ttl=True,
        return_last_access=True,
        return_value=True,
        return_fetched=True,
        return_client_flag=True,
        return_cas_token=True,
        vivify_on_miss_ttl=30,
    ),
    "failure_handling": DEFAULT_FAILURE_HANDLING,
}

# Shared by every hot cache under test, so the numbers in the assertions
# mean the same thing everywhere.
CACHE_TTL = 60
MAX_STALE_WHILE_REVALIDATE_SECONDS = 10
HARD_DEADLINE = CACHE_TTL + MAX_STALE_WHILE_REVALIDATE_SECONDS

DEFAULT_SETTINGS: Dict[str, Any] = {
    "cache_ttl": CACHE_TTL,
    "max_last_access_age_seconds": 10,
    "probability_factor": 1,
    "max_stale_while_revalidate_seconds": MAX_STALE_WHILE_REVALIDATE_SECONDS,
}


# The two shapes a hot cache lookup comes back in when it holds something.
# When it holds nothing at all it returns None.
def hot(value: Any) -> HotCacheLookup:
    """Served from the hot cache, no revalidation needed."""
    return HotCacheLookup(value=value, must_revalidate=False)


def revalidating(value: Any) -> HotCacheLookup:
    """Elected to refresh it, holding `value` to fall back on if that fails."""
    return HotCacheLookup(value=value, must_revalidate=True)


def make_client() -> Mock:
    """A client where `*hot` keys look hot, and `*miss` keys are missing."""

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


def make_lease_client() -> Mock:
    """A client that answers the vivify-on-miss (lease) variants of a get.

    With the N flag the server always replies with a Value, never a Miss:
    either the data, or a zero-sized placeholder flagged as won (W) or
    lost (Z).
    """

    def meta_get(
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> ReadResponse:
        if key.key.endswith("win"):
            # We got the lease: empty placeholder, we must repopulate.
            return Value(size=0, value=None, flags=ResponseFlags(win=True))
        elif key.key.endswith("lost"):
            # Someone else holds the lease. The placeholder looks hot
            # (fetched, recently accessed) but must never be promoted.
            return Value(
                size=0,
                value=None,
                flags=ResponseFlags(win=False, fetched=True, last_access=1),
            )
        elif key.key.endswith("hot"):
            return Value(
                size=1, value=1, flags=ResponseFlags(fetched=True, last_access=1)
            )
        else:
            return Value(
                size=1, value=1, flags=ResponseFlags(fetched=True, last_access=9999)
            )

    mock = Mock(spec=CacheClient)
    mock.meta_get.side_effect = meta_get
    return mock


@dataclass(frozen=True)
class HotCacheHarness:
    """Builds hot caches of one implementation over a single backing store.

    Every cache `build()` returns shares the same store, so they stand in
    for the threads (or, for sqlite, the workers) of a single deployment.
    """

    name: str
    build: Callable[..., ProbabilisticHotCache]
    count: Callable[[], int]
    contains: Callable[[str], bool]


def memory_harness() -> HotCacheHarness:
    store: Dict[str, CachedValue] = {}

    def build(client: Mock, **overrides: Any) -> ProbabilisticHotCache:
        return ProbabilisticHotCache(
            client=client,
            store=store,
            # Pickle everything, to match what sharing a sqlite db forces.
            **{**DEFAULT_SETTINGS, "immutable_types": [], **overrides},
        )

    return HotCacheHarness(
        name="memory",
        build=build,
        count=lambda: len(store),
        contains=lambda key: key in store,
    )


def sqlite_harness(db_path: str) -> HotCacheHarness:
    db = HotCacheDBConfig.initialize(db_path)

    def build(client: Mock, **overrides: Any) -> ProbabilisticHotCache:
        return SqliteProbabilisticHotCache(
            client=client,
            db=db,
            # Purging is best effort and on a timer of its own: keep it
            # out of the way so assertions on the store are deterministic.
            **{**DEFAULT_SETTINGS, "purge_interval_seconds": 1 << 30, **overrides},
        )

    def query(sql: str, *args: Any) -> Any:
        conn = sqlite3.connect(db.db_path)
        try:
            return conn.execute(sql, args).fetchone()[0]
        finally:
            conn.close()

    return HotCacheHarness(
        name="sqlite",
        build=build,
        count=lambda: query("SELECT COUNT(*) FROM hot_cache"),
        contains=lambda key: bool(
            query("SELECT COUNT(*) FROM hot_cache WHERE key = ?", key)
        ),
    )


# Metrics each implementation keeps on its own terms, so they are not part
# of the shared contract: the item_count gauge is updated on every store in
# memory but only on the periodic purge in sqlite, and only the sqlite store
# can fail in ways that are swallowed and counted as errors.
NOT_SHARED_METRICS = ("item_count", "errors")


def counters(metrics_collector: BaseMetricsCollector) -> Dict[str, int]:
    """The metric counters that every implementation must agree on."""
    return {
        name: value
        for name, value in metrics_collector.get_counters().items()
        if not name.endswith(NOT_SHARED_METRICS)
    }
