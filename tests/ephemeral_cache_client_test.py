from unittest.mock import call
from meta_memcache.protocol import NOOP, RequestFlags

from pytest_mock import MockerFixture

from meta_memcache import (
    Key,
    ServerAddress,
    CacheClient,
    connection_pool_factory_builder,
)


def test_ephemeral_cache_client(mocker: MockerFixture) -> None:
    socket = mocker.patch("meta_memcache.configuration.socket", autospec=True)
    c = socket.socket()
    cache_client = CacheClient.ephemeral_cache_client_from_servers(
        servers=[
            ServerAddress(host="ok1", port=11211),
            ServerAddress(host="ok2", port=11211),
        ],
        max_ttl=60,
        connection_pool_factory_fn=connection_pool_factory_builder(initial_pool_size=2),
    )

    cache_client.set(key=Key("foo"), value=1, ttl=1000, no_reply=True)
    c.sendall.assert_called_once_with(b"ms foo 1 q T60 F2\r\n1\r\n" + NOOP)
    c.sendall.reset_mock()

    cache_client.meta_multiget(
        keys=[Key("bar"), Key("foo")],
        flags=RequestFlags(
            no_reply=True,
            cache_ttl=1000,
        ),
    )
    c.sendall.assert_has_calls([call(b"mg bar q T60\r\n"), call(b"mg foo q T60\r\n")])
    c.sendall.reset_mock()
