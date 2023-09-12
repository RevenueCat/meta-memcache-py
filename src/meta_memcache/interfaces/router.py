from typing import Dict, List, Optional, Protocol, Set
from meta_memcache.configuration import ServerAddress
from meta_memcache.connection.pool import PoolCounters
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


class Router(Protocol):
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
        ...  # pragma: no cover

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
        ...  # pragma: no cover

    @property
    def executor(self) -> Executor:
        ...  # pragma: no cover

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        ...  # pragma: no cover


class HasRouter(Protocol):
    @property
    def router(self) -> Router:
        ...  # pragma: no cover
