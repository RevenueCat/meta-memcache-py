from typing import Dict, List, Protocol

from uhashring import HashRing  # type: ignore

from meta_memcache.connection.pool import ConnectionPool, PoolCounters
from meta_memcache.configuration import ServerAddress
from meta_memcache.protocol import Key


class ConnectionPoolProvider(Protocol):
    def get_pool(self, key: Key) -> ConnectionPool:
        ...  # pragma: no cover

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        ...  # pragma: no cover


class HostConnectionPoolProvider:
    def __init__(
        self,
        server_address: ServerAddress,
        connection_pool: ConnectionPool,
    ) -> None:
        self._server_address = server_address
        self._connection_pool = connection_pool

    def get_pool(self, key: Key) -> ConnectionPool:
        return self._connection_pool

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        return {
            self._server_address: self._connection_pool.get_counters(),
        }


class HashRingConnectionPoolProvider:
    def __init__(
        self,
        server_pool: Dict[ServerAddress, ConnectionPool],
    ) -> None:
        self._server_pool = server_pool
        self._servers: List[ServerAddress] = list(sorted(server_pool.keys()))
        self._ring: HashRing = HashRing(self._servers)

    def get_pool(self, key: Key) -> ConnectionPool:
        routing_key = key.routing_key or key.key
        server: ServerAddress = self._ring.get_node(routing_key)
        return self._server_pool[server]

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        return {
            server: pool.get_counters() for server, pool in self._server_pool.items()
        }
