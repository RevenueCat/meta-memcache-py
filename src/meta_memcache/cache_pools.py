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
        raise_on_server_error: bool = True,
    ) -> None:
        super().__init__(
            serializer=serializer or MixedSerializer(),
            binary_key_encoding_fn=binary_key_encoding_fn,
            raise_on_server_error=raise_on_server_error,
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
        raise_on_server_error: bool = True,
    ) -> None:
        super().__init__(
            server_pool=server_pool,
            serializer=serializer,
            binary_key_encoding_fn=binary_key_encoding_fn,
            raise_on_server_error=raise_on_server_error,
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
        raise_on_server_error: bool = True,
    ) -> "ShardedCachePool":
        server_pool: Dict[ServerAddress, ConnectionPool] = {
            server: connection_pool_factory_fn(server) for server in servers
        }
        return cls(
            server_pool=server_pool,
            serializer=serializer,
            binary_key_encoding_fn=binary_key_encoding_fn,
            raise_on_server_error=raise_on_server_error,
        )


class ShardedWithGutterCachePool(ShardedCachePool):
    def __init__(
        self,
        server_pool: Dict[ServerAddress, ConnectionPool],
        gutter_server_pool: Dict[ServerAddress, ConnectionPool],
        gutter_ttl: int,
        serializer: Optional[BaseSerializer] = None,
        binary_key_encoding_fn: Callable[[Key], bytes] = default_binary_key_encoding,
        raise_on_server_error: bool = True,
    ) -> None:
        super().__init__(
            server_pool=server_pool,
            serializer=serializer,
            binary_key_encoding_fn=binary_key_encoding_fn,
            raise_on_server_error=raise_on_server_error,
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
        raise_on_server_error: bool = True,
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
            raise_on_server_error=raise_on_server_error,
        )

    def _get_gutter_pool(self, key: Key) -> ConnectionPool:
        routing_key = key.routing_key or key.key
        server = self._gutter_ring.get_node(routing_key)
        return self._gutter_server_pool[server]

    def _adjust_int_flags_for_gutter(
        self,
        int_flags: Optional[Dict[IntFlag, int]],
    ) -> Optional[Dict[IntFlag, int]]:
        if int_flags:
            for flag in (
                IntFlag.CACHE_TTL,
                IntFlag.RECACHE_TTL,
                IntFlag.MISS_LEASE_TTL,
            ):
                ttl = int_flags.get(flag)
                if ttl is not None and (ttl == 0 or ttl > self._gutter_ttl):
                    int_flags[flag] = self._gutter_ttl

        return int_flags

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
        """
        try:
            return self._exec_on_pool(
                pool=self._get_pool(key),
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
            int_flags = self._adjust_int_flags_for_gutter(int_flags)
            return self._exec_on_pool(
                pool=self._get_gutter_pool(key),
                command=command,
                key=key,
                value=value,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
                # We don't need to track write failures on gutter, since it has
                # limited TTL already in place
                track_write_failures=False,
                raise_on_server_error=self._raise_on_server_error,
            )

    def _exec_multi(
        self,
        command: MetaCommand,
        keys: List[Key],
        values: Optional[List[bytes]] = None,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Dict[Key, MemcacheResponse]:
        """
        Groups keys by destination, gets a connection and executes the commands
        """
        results: Dict[Key, MemcacheResponse] = {}
        gutter_keys: List[Key] = []
        gutter_values: Optional[List[bytes]] = [] if values is not None else None
        for pool, key_values in self._exec_multi_prepare_pool_map(
            self._get_pool, keys, values
        ).items():
            try:
                results.update(
                    self._exec_multi_on_pool(
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
            int_flags = self._adjust_int_flags_for_gutter(int_flags)
            for pool, key_values in self._exec_multi_prepare_pool_map(
                self._get_gutter_pool, keys, gutter_values
            ).items():
                results.update(
                    self._exec_multi_on_pool(
                        pool=pool,
                        command=command,
                        key_values=key_values,
                        flags=flags,
                        int_flags=int_flags,
                        token_flags=token_flags,
                        # We don't need to track write failures on gutter, since it has
                        # limited TTL already in place
                        track_write_failures=False,
                        raise_on_server_error=self._raise_on_server_error,
                    )
                )
        return results
