from typing import Any, Dict, List, Optional

from meta_memcache.commands.high_level_commands import HighLevelCommandsMixin
from meta_memcache.configuration import ServerAddress
from meta_memcache.connection.pool import PoolCounters
from meta_memcache.interfaces.router import FailureHandling, DEFAULT_FAILURE_HANDLING
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.protocol import (
    Key,
    ReadResponse,
    RequestFlags,
    WriteResponse,
)


class ClientWrapper(HighLevelCommandsMixin):
    """
    Wraps a CacheClient wiring the meta-commands to the real client

    This is useful to extend and use in wrappers that want to add
    some features wrapping the real client instead of extending it.
    """

    def __init__(
        self,
        client: CacheApi,
    ) -> None:
        self.client = client
        self.on_write_failure = client.on_write_failure

    def meta_multiget(
        self,
        keys: List[Key],
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> Dict[Key, ReadResponse]:
        return self.client.meta_multiget(
            keys=keys,
            flags=flags,
            failure_handling=failure_handling,
        )

    def meta_get(
        self,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> ReadResponse:
        return self.client.meta_get(
            key=key,
            flags=flags,
            failure_handling=failure_handling,
        )

    def meta_set(
        self,
        key: Key,
        value: Any,
        ttl: int,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        return self.client.meta_set(
            key=key,
            value=value,
            ttl=ttl,
            flags=flags,
            failure_handling=failure_handling,
        )

    def meta_delete(
        self,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        return self.client.meta_delete(
            key=key,
            flags=flags,
            failure_handling=failure_handling,
        )

    def meta_arithmetic(
        self,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        return self.client.meta_arithmetic(
            key=key,
            flags=flags,
            failure_handling=failure_handling,
        )

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        return self.client.get_counters()
