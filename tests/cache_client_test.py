import random
from typing import Tuple
from unittest.mock import call

from pytest_mock import MockerFixture

from meta_memcache import (
    CacheClient,
    Key,
    ServerAddress,
    connection_pool_factory_builder,
)
from meta_memcache.connection.pool import ConnectionPool, PoolCounters
from meta_memcache.errors import MemcacheServerError
from meta_memcache.protocol import NOOP, RequestFlags
from meta_memcache.settings import DEFAULT_MARK_DOWN_PERIOD_S


def test_sharded_cache_client(mocker: MockerFixture) -> None:
    mocker.patch("meta_memcache.configuration.socket", autospec=True)
    cache_client = CacheClient.cache_client_from_servers(
        servers=[
            ServerAddress(host="1.1.1.1", port=11211),
            ServerAddress(host="2.2.2.2", port=11211),
            ServerAddress(host="3.3.3.3", port=11211),
        ],
        connection_pool_factory_fn=connection_pool_factory_builder(),
    )
    connection_pool = cache_client.router.pool_provider.get_pool(Key("foo"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool.server == "2.2.2.2:11211"

    connection_pool = cache_client.router.pool_provider.get_pool(Key("bar"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool.server == "1.1.1.1:11211"

    connection_pool = cache_client.router.pool_provider.get_pool(
        Key("bar", routing_key="foo")
    )
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool.server == "2.2.2.2:11211"


def test_sharded_cache_client_different_order_same_results(
    mocker: MockerFixture,
) -> None:
    mocker.patch("meta_memcache.configuration.socket", autospec=True)
    server_addresses = [
        ServerAddress(host="1.1.1.1", port=11211),
        ServerAddress(host="2.2.2.2", port=11211),
        ServerAddress(host="3.3.3.3", port=11211),
    ]
    random.shuffle(server_addresses)
    cache_client = CacheClient.cache_client_from_servers(
        servers=server_addresses,
        connection_pool_factory_fn=connection_pool_factory_builder(),
    )
    connection_pool = cache_client.router.pool_provider.get_pool(Key("foo"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool.server == "2.2.2.2:11211"

    connection_pool = cache_client.router.pool_provider.get_pool(Key("bar"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool.server == "1.1.1.1:11211"

    connection_pool = cache_client.router.pool_provider.get_pool(
        Key("bar", routing_key="foo")
    )
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool.server == "2.2.2.2:11211"


def test_sharded_cache_client_honors_server_id(mocker: MockerFixture) -> None:
    mocker.patch("meta_memcache.configuration.socket", autospec=True)
    server_addresses = [
        ServerAddress(host="1.1.1.1", port=11211, server_id="1"),
        ServerAddress(host="2.2.2.2", port=11211, server_id="2"),
    ]
    cache_client_a = CacheClient.cache_client_from_servers(
        servers=server_addresses,
        connection_pool_factory_fn=connection_pool_factory_builder(),
    )
    server_addresses = [
        ServerAddress(host="1.1.1.1", port=11211, server_id="2"),
        ServerAddress(host="2.2.2.2", port=11211, server_id="1"),
    ]
    cache_client_b = CacheClient.cache_client_from_servers(
        servers=server_addresses,
        connection_pool_factory_fn=connection_pool_factory_builder(),
    )
    connection_pool = cache_client_a.router.pool_provider.get_pool(Key("foo"))
    assert connection_pool.server == "1"
    connection_pool = cache_client_b.router.pool_provider.get_pool(Key("foo"))
    assert connection_pool.server == "1"


def test_sharded_with_gutter_cache_client(mocker: MockerFixture) -> None:
    server_is_bad = True

    def connect(server_address: Tuple[str, int]) -> None:
        host, port = server_address
        if server_is_bad and host.startswith("ko"):
            raise MemcacheServerError(server=f"{host}:{port}", message="uh-oh")

    time = mocker.patch("meta_memcache.connection.pool.time")
    time.time.return_value = 123
    socket = mocker.patch("meta_memcache.configuration.socket", autospec=True)
    c = socket.socket()
    c.connect.side_effect = connect
    cache_client = CacheClient.cache_client_with_gutter_from_servers(
        servers=[
            ServerAddress(host="ok1", port=11211),
            ServerAddress(host="ko2", port=11211),
            ServerAddress(host="ok3", port=11211),
        ],
        gutter_servers=[
            ServerAddress(host="gutter1", port=11211),
            ServerAddress(host="gutter2", port=11211),
            ServerAddress(host="gutter3", port=11211),
        ],
        gutter_ttl=60,
        connection_pool_factory_fn=connection_pool_factory_builder(initial_pool_size=2),
    )

    connection_pool = cache_client.router.pool_provider.get_pool(Key("foo"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool.server == "ko2:11211"
    assert len(connection_pool._pool) == 0
    assert connection_pool.get_counters() == PoolCounters(
        available=0,
        active=0,
        stablished=0,
        total_created=0,
        total_errors=1,  # The second conn is not attempted, marked down
    )

    connection_pool = cache_client.router.pool_provider.get_pool(Key("bar"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool.server == "ok1:11211"
    assert len(connection_pool._pool) == 2
    assert connection_pool.get_counters() == PoolCounters(
        available=2,
        active=0,
        stablished=2,
        total_created=2,
        total_errors=0,
    )

    get_pool = mocker.spy(cache_client.router.pool_provider, "get_pool")
    get_gutter_pool = mocker.spy(cache_client.router.gutter_pool_provider, "get_pool")

    cache_client.set(key=Key("bar"), value=1, ttl=1000, no_reply=True)
    get_pool.assert_called_once_with(Key("bar"))
    get_gutter_pool.assert_not_called()
    c.sendall.assert_called_once_with(b"ms bar 1 q T1000 F2\r\n1\r\n" + NOOP)
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()

    cache_client.set(key=Key("foo"), value=1, ttl=1000, no_reply=True)
    get_pool.assert_called_once_with(Key("foo"))
    get_gutter_pool.assert_called_once_with(Key("foo"))
    c.sendall.assert_called_once_with(b"ms foo 1 q T60 F2\r\n1\r\n" + NOOP)
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()

    cache_client.meta_multiget(
        keys=[Key("bar"), Key("foo")],
        flags=RequestFlags(
            no_reply=True,
            cache_ttl=1000,
        ),
    )
    get_pool.assert_has_calls([call(Key(key="bar")), call(Key(key="foo"))])
    get_gutter_pool.assert_called_once_with(Key("foo"))
    c.sendall.assert_has_calls([call(b"mg bar q T1000\r\n"), call(b"mg foo q T60\r\n")])
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()

    # Server recovers, but it is still marked down and requests
    # routed to gutter
    server_is_bad = False
    time.time.return_value = 123 + 1

    cache_client.set(key=Key("foo"), value=1, ttl=1000, no_reply=True)
    get_pool.assert_called_once_with(Key("foo"))
    get_gutter_pool.assert_called_once_with(Key("foo"))
    c.sendall.assert_called_once_with(b"ms foo 1 q T60 F2\r\n1\r\n" + NOOP)
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()

    cache_client.meta_multiget(
        keys=[Key("bar"), Key("foo")],
        flags=RequestFlags(
            no_reply=True,
            cache_ttl=1000,
        ),
    )
    get_pool.assert_has_calls([call(Key(key="bar")), call(Key(key="foo"))])
    get_gutter_pool.assert_called_once_with(Key("foo"))
    c.sendall.assert_has_calls([call(b"mg bar q T1000\r\n"), call(b"mg foo q T60\r\n")])
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()

    # After DEFAULT_MARK_DOWN_PERIOD_S we will connect again
    server_is_bad = False
    time.time.return_value = 123 + DEFAULT_MARK_DOWN_PERIOD_S

    cache_client.set(key=Key("foo"), value=1, ttl=1000, no_reply=True)
    get_pool.assert_called_once_with(Key("foo"))
    get_gutter_pool.assert_not_called()
    c.sendall.assert_called_once_with(b"ms foo 1 q T1000 F2\r\n1\r\n" + NOOP)
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()

    cache_client.meta_multiget(
        keys=[Key("bar"), Key("foo")],
        flags=RequestFlags(
            no_reply=True,
            cache_ttl=1000,
        ),
    )
    get_pool.assert_has_calls([call(Key(key="bar")), call(Key(key="foo"))])
    get_gutter_pool.assert_not_called()
    c.sendall.assert_has_calls(
        [call(b"mg bar q T1000\r\n"), call(b"mg foo q T1000\r\n")]
    )
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()
