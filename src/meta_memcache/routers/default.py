from collections import defaultdict
from typing import Callable, DefaultDict, Dict, List, Optional, Set, Tuple
from meta_memcache.configuration import ServerAddress

from meta_memcache.connection.pool import ConnectionPool, PoolCounters
from meta_memcache.connection.providers import ConnectionPoolProvider
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


class DefaultRouter:
    def __init__(
        self,
        pool_provider: ConnectionPoolProvider,
        executor: Executor,
    ) -> None:
        self.pool_provider = pool_provider
        self.executor = executor

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
        Gets a connection for the key and executes the command

        You can override to implement retries, having
        a fallback pool, etc.
        """
        return self.executor.exec_on_pool(
            pool=self.pool_provider.get_pool(key),
            command=command,
            key=key,
            value=value,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
            track_write_failures=True,
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
        for pool, key_values in self._exec_multi_prepare_pool_map(
            self.pool_provider.get_pool, keys, values
        ).items():
            results.update(
                self.executor.exec_multi_on_pool(
                    pool=pool,
                    command=command,
                    key_values=key_values,
                    flags=flags,
                    int_flags=int_flags,
                    token_flags=token_flags,
                    track_write_failures=True,
                )
            )
        return results

    def _exec_multi_prepare_pool_map(
        self,
        pool_getter: Callable[[Key], ConnectionPool],
        keys: List[Key],
        values: MaybeValues = None,
    ) -> Dict[ConnectionPool, List[Tuple[Key, MaybeValue]]]:
        if values is not None and len(values) != len(keys):
            raise ValueError("Values, if provided, needs to match the number of keys")
        pool_map: DefaultDict[
            ConnectionPool, List[Tuple[Key, MaybeValue]]
        ] = defaultdict(list)
        for i, key in enumerate(keys):
            pool_map[pool_getter(key)].append((key, values[i] if values else None))
        return pool_map

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        return self.pool_provider.get_counters()
