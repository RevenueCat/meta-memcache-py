from typing import Dict, List, NamedTuple, Optional, Protocol


from meta_memcache.configuration import ServerAddress
from meta_memcache.connection.pool import PoolCounters
from meta_memcache.interfaces.executor import Executor
from meta_memcache.protocol import (
    Key,
    MaybeValue,
    MaybeValues,
    MemcacheResponse,
    MetaCommand,
    RequestFlags,
)


class FailureHandling(NamedTuple):
    """
    Override the default failure handling for a execution
    """

    # None means use the default for the pool
    raise_on_server_error: Optional[bool] = None
    track_write_failures: bool = True


DEFAULT_FAILURE_HANDLING = FailureHandling()


class Router(Protocol):
    def exec(
        self,
        command: MetaCommand,
        key: Key,
        value: MaybeValue = None,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
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
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
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
