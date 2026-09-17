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
    is_error_response,
)
from meta_memcache.routers.default import DefaultRouter
from meta_memcache.routers.helpers import adjust_flags_for_max_ttl


class GutterRouter(DefaultRouter):
    def __init__(
        self,
        pool_provider: ConnectionPoolProvider,
        gutter_pool_provider: ConnectionPoolProvider,
        gutter_ttl: int,
        executor: Executor,
    ) -> None:
        super().__init__(
            pool_provider=pool_provider,
            executor=executor,
        )
        self.gutter_pool_provider = gutter_pool_provider
        self._gutter_ttl = gutter_ttl

    def exec(
        self,
        command: MetaCommand,
        key: Key,
        value: MaybeValue = None,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> MemcacheResponse:
        """
        Implements the gutter logic

        Tries on regular pool. On memcache server error, it
        tries in the gutter pool adjusting the TTLs so keys
        expire soon.
        """
        result = self.executor.exec_on_pool(
            pool=self.pool_provider.get_pool(key),
            command=command,
            key=key,
            value=value,
            flags=flags,
            # We never raise on the regular pool, the failure comes back as a
            # marker response so we can try the gutter pool
            raise_on_server_error=False,
            # On the regular pool, respect the track_write_failures flag
            track_write_failures=failure_handling.track_write_failures,
        )
        if not is_error_response(result):
            return result

        # Override TTLs > than gutter TTL
        flags = adjust_flags_for_max_ttl(flags, self._gutter_ttl)
        return self.executor.exec_on_pool(
            pool=self.gutter_pool_provider.get_pool(key),
            command=command,
            key=key,
            value=value,
            flags=flags,
            # Respect the raise_on_server_error flag if the gutter pool also
            # fails
            raise_on_server_error=failure_handling.raise_on_server_error,
            # On the gutter pool we never need to track write failures, since
            # it has limited TTL already in place
            track_write_failures=False,
        )

    def exec_multi(
        self,
        command: MetaCommand,
        keys: List[Key],
        values: MaybeValues = None,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> Dict[Key, MemcacheResponse]:
        """
        Implements the gutter logic for multi-key commands

        Tries on the regular pools. The keys that hit a memcache server error
        are retried on the gutter pools, adjusting the TTLs so they expire
        soon.
        """
        results = self._exec_multi_on_provider(
            self.pool_provider,
            command=command,
            keys=keys,
            values=values,
            flags=flags,
            # We never raise on the regular pools, the failures come back as
            # marker responses so we can try the gutter pools
            raise_on_server_error=False,
            # On the regular pools, respect the track_write_failures flag
            track_write_failures=failure_handling.track_write_failures,
        )

        gutter_keys: List[Key] = []
        gutter_values: MaybeValues = [] if values is not None else None
        for i, key in enumerate(keys):
            if is_error_response(results[key]):
                gutter_keys.append(key)
                if gutter_values is not None and values is not None:
                    gutter_values.append(values[i])
        if gutter_keys:
            results.update(
                self._exec_multi_on_provider(
                    self.gutter_pool_provider,
                    command=command,
                    keys=gutter_keys,
                    values=gutter_values,
                    # Override TTLs > than gutter TTL
                    flags=adjust_flags_for_max_ttl(flags, self._gutter_ttl),
                    # Respect the raise_on_server_error flag if the gutter pools
                    # also fail
                    raise_on_server_error=failure_handling.raise_on_server_error,
                    # On the gutter pools we never need to track write failures,
                    # since they have limited TTL already in place
                    track_write_failures=False,
                )
            )
        return results
