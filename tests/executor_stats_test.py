from typing import Callable, List, Optional
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from meta_memcache import (
    CacheClient,
    Key,
    ServerAddress,
    connection_pool_factory_builder,
)
from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.connection.providers import HostConnectionPoolProvider
from meta_memcache.errors import MemcacheServerError
from meta_memcache.executors.default import DefaultExecutor
from meta_memcache.protocol import (
    MISS_DUE_TO_ERROR,
    MetaCommand,
    Miss,
    ResponseFlags,
    ServerVersion,
    Success,
    Value,
)
from meta_memcache.routers.default import DefaultRouter
from meta_memcache.serializer import MixedSerializer
from meta_memcache.stats import CacheStats, StatsSampler


@pytest.fixture
def memcache_socket(mocker: MockerFixture) -> MagicMock:
    memcache_socket = mocker.MagicMock(spec=MemcacheSocket)
    memcache_socket.get_version.return_value = ServerVersion.STABLE
    return memcache_socket


@pytest.fixture
def stats() -> List[CacheStats]:
    return []


def build_client(
    memcache_socket: MagicMock,
    stats_callback: Optional[Callable[[CacheStats], None]],
    raise_on_server_error: bool = True,
    stats_sampler: Optional[StatsSampler] = None,
) -> CacheClient:
    pool = MagicMock(spec=ConnectionPool)
    pool.server = "test:11211"
    pool.pop_connection.return_value = memcache_socket
    executor = DefaultExecutor(
        serializer=MixedSerializer(),
        raise_on_server_error=raise_on_server_error,
        stats_callback=stats_callback,
        stats_sampler=stats_sampler,
    )
    router = DefaultRouter(
        pool_provider=HostConnectionPoolProvider(
            server_address=ServerAddress("test", 11211),
            connection_pool=pool,
        ),
        executor=executor,
    )
    return CacheClient(router=router)


def binary_value(data: bytes) -> Value:
    return Value(
        size=len(data),
        value=data,
        flags=ResponseFlags(client_flag=MixedSerializer.BINARY),
    )


def test_reports_each_read_sent_to_the_server(
    memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    response = binary_value(b"bar")
    memcache_socket.meta_get.return_value = response
    client = build_client(memcache_socket, stats.append)

    assert client.get("foo") == b"bar"

    [reported] = stats
    assert reported.command == MetaCommand.META_GET
    assert reported.keys == [Key("foo")]
    assert reported.size == 3
    assert reported.server == "test:11211"
    assert reported.flags is not None and reported.flags.return_value
    assert reported.responses == {Key("foo"): response}
    assert reported.start_time_ns > 0
    assert reported.duration_ns >= 0
    assert reported.error is None
    assert reported.weight == 1  # Without a sampler, everything is reported


def test_reports_a_multi_get_once_per_server(
    memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    memcache_socket.meta_multiget.return_value = [binary_value(b"bar"), Miss()]
    client = build_client(memcache_socket, stats.append)

    assert client.multi_get(["a", "b"]) == {Key("a"): b"bar", Key("b"): None}

    [reported] = stats
    assert reported.command == MetaCommand.META_GET
    assert reported.keys == [Key("a"), Key("b")]
    assert reported.size == 3
    assert reported.responses is not None
    assert isinstance(reported.responses[Key("b")], Miss)


def test_reports_writes(memcache_socket: MagicMock, stats: List[CacheStats]) -> None:
    memcache_socket.meta_set.return_value = Success(flags=ResponseFlags())
    client = build_client(memcache_socket, stats.append)

    assert client.set("foo", b"bar", ttl=60)

    [reported] = stats
    assert reported.command == MetaCommand.META_SET
    assert reported.keys == [Key("foo")]
    assert reported.size == 0
    assert reported.responses is not None
    assert isinstance(reported.responses[Key("foo")], Success)


def test_reports_a_raised_failure_with_its_error(
    memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    memcache_socket.meta_get.side_effect = OSError("mimic socket error")
    client = build_client(memcache_socket, stats.append)

    with pytest.raises(MemcacheServerError):
        client.get("foo")

    [reported] = stats
    assert reported.keys == [Key("foo")]
    assert isinstance(reported.error, MemcacheServerError)
    assert reported.responses is None
    assert reported.size == 0


def test_reports_a_failure_it_does_not_raise_as_an_error_marker(
    memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    memcache_socket.meta_get.side_effect = OSError("mimic socket error")
    client = build_client(memcache_socket, stats.append, raise_on_server_error=False)

    assert client.get("foo") is None

    [reported] = stats
    assert reported.error is None
    assert reported.responses == {Key("foo"): MISS_DUE_TO_ERROR}


def test_a_failing_stats_callback_does_not_fail_the_operation(
    memcache_socket: MagicMock,
) -> None:
    def fail(stats: CacheStats) -> None:
        raise RuntimeError("mimic broken callback")

    memcache_socket.meta_get.return_value = binary_value(b"bar")
    client = build_client(memcache_socket, fail)

    assert client.get("foo") == b"bar"


def test_cache_client_builders_take_a_stats_callback(
    mock_memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    client = CacheClient.cache_client_from_servers(
        servers=[ServerAddress(host="1.1.1.1", port=11211)],
        connection_pool_factory_fn=connection_pool_factory_builder(),
        stats_callback=stats.append,
    )

    assert client.get("foo") is None
    assert [(s.server, s.keys) for s in stats] == [("1.1.1.1:11211", [Key("foo")])]


def test_a_sampler_skips_the_operations_it_returns_no_weight_for(
    memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    weights = iter([None, 10, None])
    memcache_socket.meta_get.return_value = binary_value(b"bar")
    client = build_client(memcache_socket, stats.append, stats_sampler=weights.__next__)

    assert client.get("a") == b"bar"
    assert client.get("b") == b"bar"
    assert client.get("c") == b"bar"

    [reported] = stats
    assert reported.keys == [Key("b")]
    assert reported.weight == 10


def test_a_sampler_samples_a_multi_get_once_per_server(
    memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    sampler = MagicMock(return_value=100)
    memcache_socket.meta_multiget.return_value = [binary_value(b"bar"), Miss()]
    client = build_client(memcache_socket, stats.append, stats_sampler=sampler)

    client.multi_get(["a", "b"])

    sampler.assert_called_once_with()
    [reported] = stats
    assert reported.keys == [Key("a"), Key("b")]
    assert reported.weight == 100


def test_a_sampler_samples_failures_too(
    memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    memcache_socket.meta_get.side_effect = OSError("mimic socket error")
    client = build_client(memcache_socket, stats.append, stats_sampler=lambda: 5)

    with pytest.raises(MemcacheServerError):
        client.get("foo")

    [reported] = stats
    assert isinstance(reported.error, MemcacheServerError)
    assert reported.weight == 5


def test_the_sampler_is_not_called_without_a_callback(
    memcache_socket: MagicMock,
) -> None:
    sampler = MagicMock(return_value=1)
    memcache_socket.meta_get.return_value = binary_value(b"bar")
    client = build_client(memcache_socket, None, stats_sampler=sampler)

    assert client.get("foo") == b"bar"
    sampler.assert_not_called()


def test_a_failing_sampler_skips_the_report_but_not_the_operation(
    memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    def fail() -> Optional[int]:
        raise RuntimeError("mimic broken sampler")

    memcache_socket.meta_get.return_value = binary_value(b"bar")
    client = build_client(memcache_socket, stats.append, stats_sampler=fail)

    assert client.get("foo") == b"bar"
    assert stats == []


def test_cache_client_builders_take_a_stats_sampler(
    mock_memcache_socket: MagicMock, stats: List[CacheStats]
) -> None:
    client = CacheClient.cache_client_from_servers(
        servers=[ServerAddress(host="1.1.1.1", port=11211)],
        connection_pool_factory_fn=connection_pool_factory_builder(),
        stats_callback=stats.append,
        stats_sampler=lambda: 3,
    )

    assert client.get("foo") is None
    assert [(s.keys, s.weight) for s in stats] == [([Key("foo")], 3)]
