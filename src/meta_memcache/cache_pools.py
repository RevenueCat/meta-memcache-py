from typing import Callable, Dict, Iterable, List, Optional, Set

from uhashring import HashRing  # type: ignore

from meta_memcache.base.cache_pool import CachePool
from meta_memcache.base.connection_pool import ConnectionPool, PoolCounters
from meta_memcache.configuration import ServerAddress, default_binary_key_encoding
from meta_memcache.errors import MemcacheServerError
from meta_memcache.protocol import (
    Flag,
    IntFlag,
    Key,
    MemcacheResponse,
    MetaCommand,
    TokenFlag,
)
from meta_memcache.serializer import BaseSerializer, MixedSerializer


class MultiServerCachePool(CachePool):
    def __init__(
        self,
        server_pool: Dict[ServerAddress, ConnectionPool],
        serializer: Optional[BaseSerializer] = None,
        binary_key_encoding_fn: Callable[[Key], bytes] = default_binary_key_encoding,
    ) -> None:
        super().__init__(
            serializer=serializer or MixedSerializer(),
            binary_key_encoding_fn=binary_key_encoding_fn,
        )
        self._server_pool = server_pool

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        return {
            server: pool.get_counters() for server, pool in self._server_pool.items()
        }


class ShardedCachePool(MultiServerCachePool):
    def __init__(
        self,
        server_pool: Dict[ServerAddress, ConnectionPool],
        serializer: Optional[BaseSerializer] = None,
        binary_key_encoding_fn: Callable[[Key], bytes] = default_binary_key_encoding,
    ) -> None:
        super().__init__(
            server_pool=server_pool,
            serializer=serializer,
            binary_key_encoding_fn=binary_key_encoding_fn,
        )
        self._servers: List[ServerAddress] = list(sorted(server_pool.keys()))
        self._ring: HashRing = HashRing(self._servers)

    def _get_pool(self, key: Key) -> ConnectionPool:
        routing_key = key.routing_key or key.key
        server = self._ring.get_node(routing_key)
        return self._server_pool[server]

    @classmethod
    def from_server_addresses(
        cls,
        servers: Iterable[ServerAddress],
        connection_pool_factory_fn: Callable[[ServerAddress], ConnectionPool],
        serializer: Optional[BaseSerializer] = None,
        binary_key_encoding_fn: Callable[[Key], bytes] = default_binary_key_encoding,
    ) -> "ShardedCachePool":
        server_pool: Dict[ServerAddress, ConnectionPool] = {
            server: connection_pool_factory_fn(server) for server in servers
        }
        return cls(
            server_pool=server_pool,
            serializer=serializer,
            binary_key_encoding_fn=binary_key_encoding_fn,
        )


class ShardedWithGutterCachePool(ShardedCachePool):
    def __init__(
        self,
        server_pool: Dict[ServerAddress, ConnectionPool],
        gutter_server_pool: Dict[ServerAddress, ConnectionPool],
        gutter_ttl: int,
        serializer: Optional[BaseSerializer] = None,
        binary_key_encoding_fn: Callable[[Key], bytes] = default_binary_key_encoding,
    ) -> None:
        super().__init__(
            server_pool=server_pool,
            serializer=serializer,
            binary_key_encoding_fn=binary_key_encoding_fn,
        )
        self._gutter_server_pool = gutter_server_pool
        self._gutter_servers: List[ServerAddress] = list(
            sorted(gutter_server_pool.keys())
        )
        self._gutter_ttl = gutter_ttl
        self._gutter_ring: HashRing = HashRing(self._gutter_servers)

    @classmethod
    def from_server_with_gutter_server_addresses(
        cls,
        servers: Iterable[ServerAddress],
        gutter_servers: Iterable[ServerAddress],
        gutter_ttl: int,
        connection_pool_factory_fn: Callable[[ServerAddress], ConnectionPool],
        serializer: Optional[BaseSerializer] = None,
        binary_key_encoding_fn: Callable[[Key], bytes] = default_binary_key_encoding,
    ) -> "ShardedWithGutterCachePool":
        server_pool: Dict[ServerAddress, ConnectionPool] = {
            server: connection_pool_factory_fn(server) for server in servers
        }
        gutter_server_pool: Dict[ServerAddress, ConnectionPool] = {
            server: connection_pool_factory_fn(server) for server in gutter_servers
        }

        return cls(
            server_pool=server_pool,
            gutter_server_pool=gutter_server_pool,
            gutter_ttl=gutter_ttl,
            serializer=serializer,
            binary_key_encoding_fn=binary_key_encoding_fn,
        )

    def _get_gutter_pool(self, key: Key) -> ConnectionPool:
        routing_key = key.routing_key or key.key
        server = self._gutter_ring.get_node(routing_key)
        return self._gutter_server_pool[server]

    def _exec(
        self,
        command: MetaCommand,
        key: Key,
        value: Optional[bytes] = None,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> MemcacheResponse:
        """
        Implements the gutter logic

        Tries on regular pool. On memcache server error, it
        tries in the gutter pool adjusting the TTLs so keys
        expire soon.

        TODO: record dirty keys
        """
        try:
            return super()._exec(
                command=command,
                key=key,
                value=value,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
        except MemcacheServerError:
            # Override TTLs > than gutter TTL
            if int_flags:
                for flag in (
                    IntFlag.CACHE_TTL,
                    IntFlag.RECACHE_TTL,
                    IntFlag.MISS_LEASE_TTL,
                ):
                    ttl = int_flags.get(flag)
                    if ttl is not None and (ttl == 0 or ttl > self._gutter_ttl):
                        int_flags[flag] = self._gutter_ttl

            with self._get_gutter_pool(key).get_connection() as c:
                self._conn_send_cmd(
                    c,
                    command=command,
                    key=key,
                    value=value,
                    flags=flags,
                    int_flags=int_flags,
                    token_flags=token_flags,
                )
                return self._conn_recv_response(c, flags=flags)
