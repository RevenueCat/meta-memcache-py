from typing import Any, Dict, List, Optional, Set

from meta_memcache.commands.high_level_commands import HighLevelCommandsMixin
from meta_memcache.configuration import ServerAddress
from meta_memcache.connection.pool import PoolCounters
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.protocol import (
    Flag,
    IntFlag,
    Key,
    ReadResponse,
    TokenFlag,
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
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Dict[Key, ReadResponse]:
        return self.client.meta_multiget(
            keys=keys,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )

    def meta_get(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> ReadResponse:
        return self.client.meta_get(
            key=key,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )

    def meta_set(
        self,
        key: Key,
        value: Any,
        ttl: int,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        return self.client.meta_set(
            key=key,
            value=value,
            ttl=ttl,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )

    def meta_delete(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        return self.client.meta_delete(
            key=key,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )

    def meta_arithmetic(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        return self.client.meta_arithmetic(
            key=key,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        return self.client.get_counters()
