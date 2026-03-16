from unittest.mock import MagicMock, call

from meta_memcache import (
    CacheClient,
    Key,
    ServerAddress,
    connection_pool_factory_builder,
)
from meta_memcache.protocol import RequestFlags


def test_ephemeral_cache_client(mock_memcache_socket: MagicMock) -> None:
    c = mock_memcache_socket

    cache_client = CacheClient.ephemeral_cache_client_from_servers(
        servers=[
            ServerAddress(host="ok1", port=11211),
            ServerAddress(host="ok2", port=11211),
        ],
        max_ttl=60,
        connection_pool_factory_fn=connection_pool_factory_builder(initial_pool_size=2),
    )

    cache_client.set(key=Key("foo"), value=1, ttl=1000, no_reply=True)
    c.sendall.assert_called_once_with(b"ms foo 1 q T60 F2\r\n1\r\n", with_noop=True)
    c.sendall.reset_mock()

    cache_client.meta_multiget(
        keys=[Key("bar"), Key("foo")],
        flags=RequestFlags(
            no_reply=True,
            cache_ttl=1000,
        ),
    )
    c.sendall.assert_has_calls(
        [
            call(b"mg bar q T60\r\n", with_noop=False),
            call(b"mg foo q T60\r\n", with_noop=False),
        ]
    )
    c.sendall.reset_mock()
