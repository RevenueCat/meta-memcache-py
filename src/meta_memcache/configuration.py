import hashlib
import socket
from typing import Callable, NamedTuple, Optional

from meta_memcache.base.connection_pool import ConnectionPool
from meta_memcache.protocol import Key, ServerVersion
from meta_memcache.settings import DEFAULT_MARK_DOWN_PERIOD_S


class ServerAddress(NamedTuple):
    host: str
    port: int
    # ShardedCachePool with HashRing uses str(Server) to
    # configure the ring of servers and the shardmap. You can
    # override this id to control the HashRing configuration,
    # for example, to mimic bmemcached sharding, you should
    # user server_id = <host>:<port>_<user>_<password>
    # If you want to replace servers in-place you can
    # use numerical server_ids, and change the destination
    # IP maintaining the server_id for clean swaps.
    server_id: Optional[str] = None
    version: ServerVersion = ServerVersion.STABLE

    def __str__(self) -> str:
        if self.server_id is not None:
            return self.server_id
        elif ":" in self.host:
            return f"[{self.host}]:{self.port}"
        else:
            return f"{self.host}:{self.port}"


def socket_factory_builder(
    host: str,
    port: int,
    connection_timeout: float,
    recv_timeout: float,
    no_delay: bool,
) -> Callable[[], socket.socket]:
    """
    Helper to generate a socket_builder with desired settings

    The ConnectionPool class requires a callback to generate new
    socket. This provides a helper to build it with the desired
    configuration, but you can create your own and add TLS,
    authentication, build unix sockets, etc.
    """

    def socket_builder() -> socket.socket:
        s: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(connection_timeout)
        s.connect((host, port))
        if recv_timeout != connection_timeout:
            s.settimeout(recv_timeout)
        if no_delay:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        return s

    return socket_builder


def connection_pool_factory_builder(
    initial_pool_size: int = 1,
    max_pool_size: int = 3,
    mark_down_period_s: float = DEFAULT_MARK_DOWN_PERIOD_S,
    connection_timeout: float = 1,
    recv_timeout: float = 1,
    no_delay: bool = True,
    read_buffer_size: int = 4096,
) -> Callable[[ServerAddress], ConnectionPool]:
    """
    Helper to generate a connection_pool_builder with desired settings

    The CachePool classes require a builder for the ConnectionPool.
    This provides a helper to configure the most usual aspects, but
    you can write and constomize it to your own.
    """

    def connection_pool_builder(server_address: ServerAddress) -> ConnectionPool:
        return ConnectionPool(
            server=str(server_address),
            socket_factory_fn=socket_factory_builder(
                host=server_address.host,
                port=server_address.port,
                connection_timeout=connection_timeout,
                recv_timeout=recv_timeout,
                no_delay=no_delay,
            ),
            initial_pool_size=initial_pool_size,
            max_pool_size=max_pool_size,
            mark_down_period_s=mark_down_period_s,
            read_buffer_size=read_buffer_size,
            version=server_address.version,
        )

    return connection_pool_builder


class RecachePolicy(NamedTuple):
    """
    This controls the recache herd control behavior

    If recache ttl is indicated, when remaining ttl is < given value
    one of the clients will win, return a miss and will populate the
    value, while the other clients will loose and continue to use the
    stale value.
    """

    ttl: int = 30


class LeasePolicy(NamedTuple):
    """
    This controls the lease or miss herd control behavior

    If miss lease retries > 0, on misses a lease will be created. The
    winner will get a Miss and will continue to populate the cache,
    while the others are BLOCKED! Use with caution! You can define
    how many times and how often clients will retry to get the
    value. After the retries are expired, clients will get a Miss
    if they weren't able to get the value.
    """

    ttl: int = 30
    miss_retries: int = 3
    miss_retry_wait: float = 1.0
    wait_backoff_factor: float = 1.2
    miss_max_retry_wait: float = 5.0


class StalePolicy(NamedTuple):
    """
    This controls the stale herd control behavior

    * Deletions can mark items stale instead of deleting them
    * Stale items automatically do recache control, one client
      will get the miss, others will receive the stale value
      until the winner refreshes the value in the cache.
    * cas mismatches (due to race / further invalidation) can
      store the value as stale instead of failing
    """

    mark_stale_on_deletion_ttl: int = 0  # 0 means disabled
    mark_stale_on_cas_mismatch: bool = False


def default_binary_key_encoding(key: Key) -> bytes:
    """
    Generate binary key from Key, used for keys with unicode,
    binary or those too long.
    """
    return hashlib.blake2b(key.key.encode(), digest_size=18).digest()
