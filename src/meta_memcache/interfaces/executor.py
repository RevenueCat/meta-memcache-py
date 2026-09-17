from typing import Dict, List, Optional, Protocol, Tuple

from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.events.write_failure_event import WriteFailureEvent

from meta_memcache.protocol import (
    Key,
    MaybeValue,
    MemcacheResponse,
    MetaCommand,
    RequestFlags,
)


class Executor(Protocol):
    """
    Executes commands on a connection pool.

    When a command fails and `raise_on_server_error` is False, implementations
    must not report the failure as an ordinary negative response: they have to
    return the MISS_DUE_TO_ERROR / NOT_STORED_DUE_TO_ERROR markers, so callers
    can tell "the server said no" from "we could not ask". Routers such as
    GutterRouter rely on this to decide when to fail over.
    """

    def exec_on_pool(
        self,
        pool: ConnectionPool,
        command: MetaCommand,
        key: Key,
        value: MaybeValue,
        flags: Optional[RequestFlags],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
    ) -> MemcacheResponse:
        """
        Executes a command on a pool

        Gets a connection for the key and executes the command.

        Returns MISS_DUE_TO_ERROR (reads) or NOT_STORED_DUE_TO_ERROR (writes)
        if the command fails and `raise_on_server_error` is False.
        """
        ...  # pragma: no cover

    def exec_multi_on_pool(
        self,
        pool: ConnectionPool,
        command: MetaCommand,
        key_values: List[Tuple[Key, MaybeValue]],
        flags: Optional[RequestFlags],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
    ) -> Dict[Key, MemcacheResponse]:
        """
        Executes a multi-key command on a pool

        Gets a connection for the key and executes the commands. Returns an
        entry for every key, using MISS_DUE_TO_ERROR (reads) or
        NOT_STORED_DUE_TO_ERROR (writes) for the ones that failed, if
        `raise_on_server_error` is False.
        """
        ...  # pragma: no cover

    @property
    def on_write_failure(self) -> WriteFailureEvent: ...  # pragma: no cover

    @on_write_failure.setter
    def on_write_failure(
        self, value: WriteFailureEvent
    ) -> None: ...  # pragma: no cover
