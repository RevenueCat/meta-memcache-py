import random
from typing import Tuple

from pytest_mock import MockerFixture

from meta_memcache import (
    IPPort,
    Key,
    ShardedCachePool,
    ShardedWithGutterCachePool,
    connection_pool_factory_builder,
)
from meta_memcache.base.connection_pool import ConnectionPool, PoolCounters
from meta_memcache.errors import MemcacheServerError
from meta_memcache.settings import DEFAULT_MARK_DOWN_PERIOD_S


def test_sharded_cache_pool(mocker: MockerFixture) -> None:
    mocker.patch("meta_memcache.configuration.socket", autospec=True)
    cache_pool = ShardedCachePool.from_ipport_list(
        servers=[
            IPPort(ip="1.1.1.1", port=11211),
            IPPort(ip="2.2.2.2", port=11211),
            IPPort(ip="3.3.3.3", port=11211),
        ],
        connection_pool_factory_fn=connection_pool_factory_builder(),
    )
    connection_pool = cache_pool._get_pool(Key("foo"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool._server == "2.2.2.2:11211"

    connection_pool = cache_pool._get_pool(Key("bar"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool._server == "1.1.1.1:11211"

    connection_pool = cache_pool._get_pool(Key("bar", routing_key="foo"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool._server == "2.2.2.2:11211"


def test_sharded_cache_pool_different_order_same_results(mocker: MockerFixture) -> None:
    mocker.patch("meta_memcache.configuration.socket", autospec=True)
    server_list = [
        IPPort(ip="1.1.1.1", port=11211),
        IPPort(ip="2.2.2.2", port=11211),
        IPPort(ip="3.3.3.3", port=11211),
    ]
    random.shuffle(server_list)
    cache_pool = ShardedCachePool.from_ipport_list(
        servers=server_list,
        connection_pool_factory_fn=connection_pool_factory_builder(),
    )
    connection_pool = cache_pool._get_pool(Key("foo"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool._server == "2.2.2.2:11211"

    connection_pool = cache_pool._get_pool(Key("bar"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool._server == "1.1.1.1:11211"

    connection_pool = cache_pool._get_pool(Key("bar", routing_key="foo"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool._server == "2.2.2.2:11211"


def test_sharded_with_gutter_cache_pool(mocker: MockerFixture) -> None:
    server_is_bad = True

    def connect(ip_port: Tuple[str, int]) -> None:
        ip, port = ip_port
        if server_is_bad and ip.startswith("ko"):
            raise MemcacheServerError(server=f"{ip}:{port}", message="uh-oh")

    time = mocker.patch("meta_memcache.base.connection_pool.time")
    time.time.return_value = 123
    socket = mocker.patch("meta_memcache.configuration.socket", autospec=True)
    c = socket.socket()
    c.connect.side_effect = connect
    cache_pool = ShardedWithGutterCachePool.from_ipport_list(
        servers=[
            IPPort(ip="ok1", port=11211),
            IPPort(ip="ko2", port=11211),
            IPPort(ip="ok3", port=11211),
        ],
        gutter_servers=[
            IPPort(ip="gutter1", port=11211),
            IPPort(ip="gutter2", port=11211),
            IPPort(ip="gutter3", port=11211),
        ],
        gutter_ttl=60,
        connection_pool_factory_fn=connection_pool_factory_builder(initial_pool_size=2),
    )

    connection_pool = cache_pool._get_pool(Key("foo"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool._server == "ko2:11211"
    assert connection_pool._pool.qsize() == 0
    assert connection_pool.get_counters() == PoolCounters(
        available=0,
        active=0,
        stablished=0,
        total_created=0,
        total_errors=1,  # The second conn is not attempted, marked down
    )

    connection_pool = cache_pool._get_pool(Key("bar"))
    assert isinstance(connection_pool, ConnectionPool)
    assert connection_pool._server == "ok1:11211"
    assert connection_pool._pool.qsize() == 2
    assert connection_pool.get_counters() == PoolCounters(
        available=2,
        active=0,
        stablished=2,
        total_created=2,
        total_errors=0,
    )

    get_pool = mocker.spy(cache_pool, "_get_pool")
    get_gutter_pool = mocker.spy(cache_pool, "_get_gutter_pool")

    cache_pool.set(key=Key("bar"), value=1, ttl=1000, no_reply=True)
    get_pool.assert_called_once_with(Key("bar"))
    get_gutter_pool.assert_not_called()
    c.sendall.assert_called_once_with(b"ms bar 1 F2 T1000 q\r\n1\r\n")
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()

    cache_pool.set(key=Key("foo"), value=1, ttl=1000, no_reply=True)
    get_pool.assert_called_once_with(Key("foo"))
    get_gutter_pool.assert_called_once_with(Key("foo"))
    c.sendall.assert_called_once_with(b"ms foo 1 F2 T60 q\r\n1\r\n")
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()

    # Server recovers, but it is still marked down and requests
    # routed to gutter
    server_is_bad = False
    time.time.return_value = 123 + 1

    cache_pool.set(key=Key("foo"), value=1, ttl=1000, no_reply=True)
    get_pool.assert_called_once_with(Key("foo"))
    get_gutter_pool.assert_called_once_with(Key("foo"))
    c.sendall.assert_called_once_with(b"ms foo 1 F2 T60 q\r\n1\r\n")
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()

    # After DEFAULT_MARK_DOWN_PERIOD_S we will connect again
    server_is_bad = False
    time.time.return_value = 123 + DEFAULT_MARK_DOWN_PERIOD_S

    cache_pool.set(key=Key("foo"), value=1, ttl=1000, no_reply=True)
    get_pool.assert_called_once_with(Key("foo"))
    get_gutter_pool.assert_not_called()
    c.sendall.assert_called_once_with(b"ms foo 1 F2 T1000 q\r\n1\r\n")
    c.sendall.reset_mock()
    get_pool.reset_mock()
    get_gutter_pool.reset_mock()
