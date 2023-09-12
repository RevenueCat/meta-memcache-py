from typing import Dict, List, Optional, Set

from meta_memcache.connection.providers import ConnectionPoolProvider
from meta_memcache.errors import MemcacheServerError
from meta_memcache.interfaces.executor import Executor
from meta_memcache.protocol import (
    Flag,
    IntFlag,
    Key,
    MaybeValue,
    MaybeValues,
    MemcacheResponse,
    MetaCommand,
    TokenFlag,
)
from meta_memcache.routers.default import DefaultRouter
from meta_memcache.routers.helpers import adjust_int_flags_for_max_ttl


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
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
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
                int_flags=int_flags,
                token_flags=token_flags,
                track_write_failures=True,
                # We always want to raise on server errors so we can
                # try the gutter pool
                raise_on_server_error=True,
            )
        except MemcacheServerError:
            # Override TTLs > than gutter TTL
            int_flags = adjust_int_flags_for_max_ttl(int_flags, self._gutter_ttl)
            return self.executor.exec_on_pool(
                pool=self.gutter_pool_provider.get_pool(key),
                command=command,
                key=key,
                value=value,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
                # We don't need to track write failures on gutter, since it has
                # limited TTL already in place
                track_write_failures=False,
            )

    def exec_multi(
        self,
        command: MetaCommand,
        keys: List[Key],
        values: MaybeValues = None,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
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
                        int_flags=int_flags,
                        token_flags=token_flags,
                        track_write_failures=True,
                        # We always want to raise on server errors so we can
                        # try the gutter pool
                        raise_on_server_error=True,
                    )
                )
            except MemcacheServerError:
                for key, value in key_values:
                    gutter_keys.append(key)
                    if gutter_values is not None and value is not None:
                        gutter_values.append(value)
        if gutter_keys:
            # Override TTLs > than gutter TTL
            int_flags = adjust_int_flags_for_max_ttl(int_flags, self._gutter_ttl)
            for pool, key_values in self._exec_multi_prepare_pool_map(
                self.gutter_pool_provider.get_pool, gutter_keys, gutter_values
            ).items():
                results.update(
                    self.executor.exec_multi_on_pool(
                        pool=pool,
                        command=command,
                        key_values=key_values,
                        flags=flags,
                        int_flags=int_flags,
                        token_flags=token_flags,
                        # We don't need to track write failures on gutter, since it has
                        # limited TTL already in place
                        track_write_failures=False,
                    )
                )
        return results
