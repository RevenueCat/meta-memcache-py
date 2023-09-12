from typing import Dict, List, Optional, Protocol, Set, Tuple
from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.events.write_failure_event import WriteFailureEvent

from meta_memcache.protocol import (
    Flag,
    IntFlag,
    Key,
    MaybeValue,
    MemcacheResponse,
    MetaCommand,
    TokenFlag,
)


class Executor(Protocol):
    def exec_on_pool(
        self,
        pool: ConnectionPool,
        command: MetaCommand,
        key: Key,
        value: MaybeValue,
        flags: Optional[Set[Flag]],
        int_flags: Optional[Dict[IntFlag, int]],
        token_flags: Optional[Dict[TokenFlag, bytes]],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
    ) -> MemcacheResponse:
        """
        Executes a command on a pool

        Gets a connection for the key and executes the command
        """
        ...  # pragma: no cover

    def exec_multi_on_pool(
        self,
        pool: ConnectionPool,
        command: MetaCommand,
        key_values: List[Tuple[Key, MaybeValue]],
        flags: Optional[Set[Flag]],
        int_flags: Optional[Dict[IntFlag, int]],
        token_flags: Optional[Dict[TokenFlag, bytes]],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
    ) -> Dict[Key, MemcacheResponse]:
        """
        Executes a multi-key command on a pool

        Gets a connection for the key and executes the commands
        """
        ...  # pragma: no cover

    @property
    def on_write_failure(self) -> WriteFailureEvent:
        ...  # pragma: no cover

    @on_write_failure.setter
    def on_write_failure(self, value: WriteFailureEvent) -> None:
        ...  # pragma: no cover
