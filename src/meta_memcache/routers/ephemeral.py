from typing import Dict, List, Optional

from meta_memcache.connection.providers import ConnectionPoolProvider
from meta_memcache.interfaces.executor import Executor
from meta_memcache.interfaces.router import DEFAULT_FAILURE_HANDLING, FailureHandling
from meta_memcache.protocol import (
    Key,
    MaybeValue,
    MaybeValues,
    MemcacheResponse,
    MetaCommand,
    RequestFlags,
)
from meta_memcache.routers.default import DefaultRouter
from meta_memcache.routers.helpers import adjust_flags_for_max_ttl


class EphemeralRouter(DefaultRouter):
    """
    Ephemeral router

    The data stored will never live more that the specified
    max_ttl. Larger ttls will be reduced on the fly.

    This is useful for testing read paths while the write
    path (and thus the invalidations and cache updates)
    are not enabled, so expiring data is the only way
    to keep cache consistent. Also for rollouts where
    not all servers will get the cache code at once, so
    new cached entries will have limited TTL. Once the
    rollout is done and all server have the right version
    and will do all the proper cache invalidations, you
    can use the default executor.
    """

    def __init__(
        self,
        max_ttl: int,
        pool_provider: ConnectionPoolProvider,
        executor: Executor,
    ) -> None:
        self._max_ttl = max_ttl
        super().__init__(
            pool_provider=pool_provider,
            executor=executor,
        )

    def exec(
        self,
        command: MetaCommand,
        key: Key,
        value: MaybeValue = None,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> MemcacheResponse:
        return super().exec(
            command=command,
            key=key,
            value=value,
            flags=adjust_flags_for_max_ttl(flags, self._max_ttl),
            failure_handling=failure_handling,
        )

    def exec_multi(
        self,
        command: MetaCommand,
        keys: List[Key],
        values: MaybeValues = None,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> Dict[Key, MemcacheResponse]:
        return super().exec_multi(
            command=command,
            keys=keys,
            values=values,
            flags=adjust_flags_for_max_ttl(flags, self._max_ttl),
            failure_handling=failure_handling,
        )
