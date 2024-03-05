from typing import Dict, List, Optional

from meta_memcache.connection.providers import ConnectionPoolProvider
from meta_memcache.errors import MemcacheServerError
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
        try:
            return self.executor.exec_on_pool(
                pool=self.pool_provider.get_pool(key),
                command=command,
                key=key,
                value=value,
                flags=flags,
                # We always want to raise on server errors so we can
                # try the gutter pool
                raise_on_server_error=True,
                # On the regular pool, respect the track_write_failures flag
                track_write_failures=failure_handling.track_write_failures,
            )
        except MemcacheServerError:
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
        Groups keys by destination, gets a connection and executes the commands
        """
        results: Dict[Key, MemcacheResponse] = {}
        gutter_keys: List[Key] = []
        gutter_values: MaybeValues = [] if values is not None else None
        for pool, key_values in self._exec_multi_prepare_pool_map(
            self.pool_provider.get_pool, keys, values
        ).items():
            try:
                results.update(
                    self.executor.exec_multi_on_pool(
                        pool=pool,
                        command=command,
                        key_values=key_values,
                        flags=flags,
                        # We always want to raise on server errors so we can
                        # try the gutter pool
                        raise_on_server_error=True,
                        # On the regular pool, respect the track_write_failures flag
                        track_write_failures=failure_handling.track_write_failures,
                    )
                )
            except MemcacheServerError:
                for key, value in key_values:
                    gutter_keys.append(key)
                    if gutter_values is not None and value is not None:
                        gutter_values.append(value)
        if gutter_keys:
            # Override TTLs > than gutter TTL
            flags = adjust_flags_for_max_ttl(flags, self._gutter_ttl)
            for pool, key_values in self._exec_multi_prepare_pool_map(
                self.gutter_pool_provider.get_pool, gutter_keys, gutter_values
            ).items():
                results.update(
                    self.executor.exec_multi_on_pool(
                        pool=pool,
                        command=command,
                        key_values=key_values,
                        flags=flags,
                        # Respect the raise_on_server_error flag if the gutter pool also
                        # fails
                        raise_on_server_error=failure_handling.raise_on_server_error,
                        # On the gutter pool we never need to track write failures,
                        # since # it has limited TTL already in place
                        track_write_failures=False,
                    )
                )
        return results
