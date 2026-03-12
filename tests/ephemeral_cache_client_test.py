from meta_memcache import (
    CacheClient,
    Key,
    ServerAddress,
    connection_pool_factory_builder,
)
from meta_memcache.protocol import RequestFlags


def test_ephemeral_cache_client(wire_socket) -> None:
    cache_client = CacheClient.ephemeral_cache_client_from_servers(
        servers=[
            ServerAddress(host="ok1", port=11211),
            ServerAddress(host="ok2", port=11211),
        ],
        max_ttl=60,
        connection_pool_factory_fn=connection_pool_factory_builder(initial_pool_size=2),
    )

    # TTL should be capped to max_ttl=60 even though we request 1000
    cache_client.set(key=Key("foo"), value=1, ttl=1000, no_reply=True)
    wire_socket.assert_wire(b"ms foo 1 q T60 F2\r\n1\r\nmn\r\n")

    # meta_multiget with no_reply — TTL also capped to 60
    cache_client.meta_multiget(
        keys=[Key("bar"), Key("foo")],
        flags=RequestFlags(
            no_reply=True,
            cache_ttl=1000,
        ),
    )
    wire_socket.assert_wire(b"mg bar q T60\r\n", b"mg foo q T60\r\n")
