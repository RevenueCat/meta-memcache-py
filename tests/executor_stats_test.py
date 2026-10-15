import itertools
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
    ValueContainer,
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


@pytest.fixture
def clock(mocker: MockerFixture) -> MagicMock:
    # Each read of the clock is 10ns after the previous one
    time = mocker.patch("meta_memcache.executors.default.time")
    time.time_ns.side_effect = itertools.count(0, 10)
    return time


def build_pool(memcache_socket: MagicMock) -> MagicMock:
    pool = MagicMock(spec=ConnectionPool)
    pool.server = "test:11211"
    pool.pop_connection.return_value = memcache_socket
    return pool


def build_client(
    memcache_socket: MagicMock,
    stats_callback: Optional[Callable[[CacheStats], None]],
    raise_on_server_error: bool = True,
    stats_sampler: Optional[StatsSampler] = None,
) -> CacheClient:
    pool = build_pool(memcache_socket)
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


def test_reports_where_a_read_spent_its_time(
    memcache_socket: MagicMock, stats: List[CacheStats], clock: MagicMock
) -> None:
    memcache_socket.meta_get.return_value = binary_value(b"bar")
    client = build_client(memcache_socket, stats.append)

    assert client.get("foo") == b"bar"

    [reported] = stats
    # Starts at 0, takes a connection until 10, round trips until 20,
    # unserializes until 30, and ends at 40
    assert reported.start_time_ns == 0
    assert reported.duration_ns == 40
    assert reported.serialize_duration_ns == 0
    assert reported.pool_duration_ns == 10
    assert reported.network_duration_ns == 10
    assert reported.deserialize_duration_ns == 10


def test_reports_where_a_multi_get_spent_its_time(
    memcache_socket: MagicMock, stats: List[CacheStats], clock: MagicMock
) -> None:
    memcache_socket.meta_multiget.return_value = [
        binary_value(b"bar"),
        Miss(),
        binary_value(b"baz"),
    ]
    client = build_client(memcache_socket, stats.append)

    client.multi_get(["a", "b", "c"])

    [reported] = stats
    # Encodes the keys until 10, takes a connection until 20, round trips
    # until 30, unserializes a and c until 40, and ends at 50
    assert reported.duration_ns == 50
    assert reported.serialize_duration_ns == 10
    assert reported.pool_duration_ns == 10
    assert reported.network_duration_ns == 10
    assert reported.deserialize_duration_ns == 10


def test_reports_where_a_write_spent_its_time(
    memcache_socket: MagicMock, stats: List[CacheStats], clock: MagicMock
) -> None:
    memcache_socket.meta_set.return_value = Success(flags=ResponseFlags())
    client = build_client(memcache_socket, stats.append)

    assert client.set("foo", b"bar", ttl=60)

    [reported] = stats
    # Serializes until 10, takes a connection until 20, round trips until 30,
    # processes the response until 40, and ends at 50
    assert reported.duration_ns == 50
    assert reported.serialize_duration_ns == 10
    assert reported.pool_duration_ns == 10
    assert reported.network_duration_ns == 10
    assert reported.deserialize_duration_ns == 10


def test_reports_where_pipelined_writes_spent_their_time(
    memcache_socket: MagicMock, stats: List[CacheStats], clock: MagicMock
) -> None:
    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())
    executor = DefaultExecutor(
        serializer=MixedSerializer(), stats_callback=stats.append
    )

    executor.exec_multi_on_pool(
        pool=build_pool(memcache_socket),
        command=MetaCommand.META_SET,
        key_values=[(Key("a"), ValueContainer(b"1")), (Key("b"), ValueContainer(b"2"))],
        flags=None,
        track_write_failures=False,
    )

    [reported] = stats
    # Serializes a and b until 10, takes a connection until 20, round trips
    # until 30, processes the responses until 40, and ends at 50
    assert reported.duration_ns == 50
    assert reported.serialize_duration_ns == 10
    assert reported.pool_duration_ns == 10
    assert reported.network_duration_ns == 10
    assert reported.deserialize_duration_ns == 10


def test_reports_the_network_time_of_a_failed_round_trip(
    memcache_socket: MagicMock, stats: List[CacheStats], clock: MagicMock
) -> None:
    memcache_socket.meta_get.side_effect = OSError("mimic socket timeout")
    client = build_client(memcache_socket, stats.append)

    with pytest.raises(MemcacheServerError):
        client.get("foo")

    [reported] = stats
    # Takes a connection until 10, fails the round trip until 20, and ends
    # at 30
    assert isinstance(reported.error, MemcacheServerError)
    assert reported.duration_ns == 30
    assert reported.pool_duration_ns == 10
    assert reported.network_duration_ns == 10
    assert reported.deserialize_duration_ns == 0


@pytest.mark.parametrize(
    "run",
    [
        lambda executor, pool: executor.exec_on_pool(
            pool=pool,
            command=MetaCommand.META_GET,
            key=Key("a"),
            value=None,
            flags=None,
            track_write_failures=False,
        ),
        lambda executor, pool: executor.exec_multi_on_pool(
            pool=pool,
            command=MetaCommand.META_GET,
            key_values=[(Key("a"), None), (Key("b"), None)],
            flags=None,
            track_write_failures=False,
        ),
        lambda executor, pool: executor.exec_multi_on_pool(
            pool=pool,
            command=MetaCommand.META_SET,
            key_values=[(Key("a"), ValueContainer(b"1"))],
            flags=None,
            track_write_failures=False,
        ),
    ],
    ids=["single", "multi_get", "pipelined"],
)
def test_reports_the_pool_time_of_a_failed_connection(
    run: Callable[[DefaultExecutor, MagicMock], object],
    memcache_socket: MagicMock,
    stats: List[CacheStats],
    clock: MagicMock,
) -> None:
    pool = build_pool(memcache_socket)
    pool.pop_connection.side_effect = MemcacheServerError(
        "test:11211", "mimic connect timeout"
    )
    executor = DefaultExecutor(
        serializer=MixedSerializer(), stats_callback=stats.append
    )

    with pytest.raises(MemcacheServerError):
        run(executor, pool)

    [reported] = stats
    # After serializing (if there is anything to serialize), fails to take a
    # connection 10 later, and ends 10 after that
    assert isinstance(reported.error, MemcacheServerError)
    assert reported.pool_duration_ns == 10
    assert reported.network_duration_ns == 0
    assert reported.deserialize_duration_ns == 0
    assert reported.duration_ns == reported.serialize_duration_ns + 20


def test_unsampled_operations_do_not_read_the_clock(
    memcache_socket: MagicMock, stats: List[CacheStats], clock: MagicMock
) -> None:
    memcache_socket.meta_get.return_value = binary_value(b"bar")
    memcache_socket.meta_set.return_value = Success(flags=ResponseFlags())
    client = build_client(memcache_socket, stats.append, stats_sampler=lambda: None)

    assert client.get("foo") == b"bar"
    assert client.set("foo", b"bar", ttl=60)

    clock.time_ns.assert_not_called()
    assert stats == []


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
