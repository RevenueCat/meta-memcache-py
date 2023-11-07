import pickle
import zlib
from dataclasses import dataclass
from typing import Callable
from unittest.mock import MagicMock, call
from meta_memcache.interfaces.router import FailureHandling

import pytest
from meta_memcache import CacheClient, Key, MemcacheError, SetMode
from meta_memcache.base.base_serializer import EncodedValue
from meta_memcache.configuration import (
    LeasePolicy,
    RecachePolicy,
    ServerAddress,
    StalePolicy,
    default_key_encoder,
)
from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.connection.providers import HostConnectionPoolProvider
from meta_memcache.errors import MemcacheServerError
from meta_memcache.events.write_failure_event import WriteFailureEvent
from meta_memcache.executors.default import DefaultExecutor
from meta_memcache.extras.client_wrapper import ClientWrapper
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.interfaces.meta_commands import MetaCommandsProtocol
from meta_memcache.protocol import (
    Miss,
    NotStored,
    ResponseFlags,
    RequestFlags,
    ServerVersion,
    Success,
    Value,
)
from meta_memcache.routers.default import DefaultRouter
from meta_memcache.serializer import MixedSerializer
from pytest_mock import MockerFixture


@dataclass
class Foo:
    bar: str


@dataclass
class Bar:
    foo: str


@pytest.fixture
def memcache_socket(mocker: MockerFixture) -> MemcacheSocket:
    memcache_socket = mocker.MagicMock(spec=MemcacheSocket)
    memcache_socket.get_version.return_value = ServerVersion.STABLE
    return memcache_socket


@pytest.fixture
def memcache_socket_1_6_6(mocker: MockerFixture) -> MemcacheSocket:
    memcache_socket = mocker.MagicMock(spec=MemcacheSocket)
    memcache_socket.get_version.return_value = ServerVersion.AWS_1_6_6
    return memcache_socket


@pytest.fixture
def connection_pool(
    mocker: MockerFixture, memcache_socket: MemcacheSocket
) -> ConnectionPool:
    connection_pool = mocker.MagicMock(spec=ConnectionPool)
    connection_pool.pop_connection.return_value = memcache_socket
    return connection_pool


@pytest.fixture
def connection_pool_1_6_6(
    mocker: MockerFixture, memcache_socket_1_6_6: MemcacheSocket
) -> ConnectionPool:
    connection_pool = mocker.MagicMock(spec=ConnectionPool)
    connection_pool.pop_connection.return_value = memcache_socket_1_6_6
    return connection_pool


def get_test_cache_client(
    connection_pool: ConnectionPool,
    raise_on_server_error: bool = True,
) -> CacheClient:
    pool_provider = HostConnectionPoolProvider(
        server_address=ServerAddress("test", 11211),
        connection_pool=connection_pool,
    )
    executor = DefaultExecutor(
        serializer=MixedSerializer(),
        key_encoder_fn=default_key_encoder,
        raise_on_server_error=raise_on_server_error,
    )
    router = DefaultRouter(
        pool_provider=pool_provider,
        executor=executor,
    )
    return CacheClient(router=router)


@pytest.fixture
def meta_command_mock(mocker: MockerFixture) -> MagicMock:
    result = mocker.MagicMock(spec=MetaCommandsProtocol)
    result.on_write_failure = WriteFailureEvent()
    return result


@pytest.fixture
def cache_client_with_mocked_meta_commands(meta_command_mock: MagicMock) -> CacheApi:
    return ClientWrapper(meta_command_mock)


@pytest.fixture
def cache_client(connection_pool: ConnectionPool) -> CacheClient:
    return get_test_cache_client(connection_pool)


@pytest.fixture
def cache_client_not_raise_on_server_error(
    connection_pool: ConnectionPool,
) -> CacheClient:
    return get_test_cache_client(connection_pool, raise_on_server_error=False)


@pytest.fixture
def cache_client_1_6_6(connection_pool_1_6_6: ConnectionPool) -> CacheClient:
    return get_test_cache_client(connection_pool_1_6_6)


def test_set_cmd(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())

    cache_client.set(key="foo", value="bar", ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F0\r\nbar\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(key=Key("foo"), value="bar", ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F0\r\nbar\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(key=Key("foo"), value=123, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    value = [1, 2, 3]
    data = pickle.dumps(value, protocol=0)
    cache_client.set(key=Key("foo"), value=value, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo " + str(len(data)).encode() + b" T300 F1\r\n" + data + b"\r\n",
        with_noop=False,
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    value = False  # Bools should be stored pickled
    data = pickle.dumps(value, protocol=0)
    cache_client.set(key=Key("foo"), value=value, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo " + str(len(data)).encode() + b" T300 F1\r\n" + data + b"\r\n",
        with_noop=False,
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(key=Key("foo"), value=b"123", ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F16\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    value = b"123" * 100
    data = zlib.compress(value)
    cache_client.set(key=Key("foo"), value=value, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo " + str(len(data)).encode() + b" T300 F24\r\n" + data + b"\r\n",
        with_noop=False,
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.ADD)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2 ME\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.APPEND)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2 MA\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.PREPEND)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2 MP\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.REPLACE)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2 MR\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(key=Key("foo"), value=b"123", ttl=300, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 q T300 F16\r\n123\r\n", with_noop=True
    )
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(key=Key("foo"), value=b"123", ttl=300, cas_token=666)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F16 C666\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(
        key=Key("foo"), value=b"123", ttl=300, cas_token=666, stale_policy=StalePolicy()
    )
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F16 C666\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.set(
        key=Key("foo"),
        value=b"123",
        ttl=300,
        cas_token=666,
        stale_policy=StalePolicy(mark_stale_on_cas_mismatch=True),
    )
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 I T300 F16 C666\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()


def test_set_cmd_1_6_6(
    memcache_socket_1_6_6: MemcacheSocket,
    cache_client_1_6_6: CacheClient,
) -> None:
    memcache_socket_1_6_6.get_response.return_value = Success(flags=ResponseFlags())

    cache_client_1_6_6.set(key="foo", value="bar", ttl=300)
    memcache_socket_1_6_6.sendall.assert_called_once_with(
        b"ms foo S3 T300 F0\r\nbar\r\n", with_noop=False
    )
    memcache_socket_1_6_6.get_response.assert_called_once_with()


def test_set_success_fail(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())
    result = cache_client.set(key=Key("foo"), value="bar", ttl=300)
    assert result is True

    memcache_socket.get_response.return_value = NotStored()
    result = cache_client.set(key=Key("foo"), value="bar", ttl=300)
    assert result is False


def test_refill(
    cache_client_with_mocked_meta_commands: CacheApi, meta_command_mock: MagicMock
) -> None:
    meta_command_mock.meta_set.return_value = Success(flags=ResponseFlags())
    cache_client_with_mocked_meta_commands.refill(key="foo", value="bar", ttl=300)
    meta_command_mock.meta_set.assert_called_once_with(
        key=Key(key="foo"),
        value="bar",
        ttl=300,
        flags=RequestFlags(cache_ttl=300, mode=SetMode.ADD.value),
        failure_handling=FailureHandling(track_write_failures=False),
    )


def test_delete_cmd(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())

    cache_client.delete(key="foo")
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.delete(key=Key("foo"))
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.delete(key=Key("foo"), cas_token=666)
    memcache_socket.sendall.assert_called_once_with(b"md foo C666\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.delete(key=Key("foo"), no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"md foo q\r\n", with_noop=True)
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.delete(key=Key("foo"), stale_policy=StalePolicy())
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.delete(
        key=Key("foo"),
        stale_policy=StalePolicy(mark_stale_on_deletion_ttl=30),
    )
    memcache_socket.sendall.assert_called_once_with(
        b"md foo I T30\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()


def test_delete_success_fail(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())
    result = cache_client.delete(key=Key("foo"))
    assert result is True

    memcache_socket.get_response.return_value = Miss()
    result = cache_client.delete(key=Key("foo"))
    assert result is False

    memcache_socket.get_response.return_value = NotStored()
    result = cache_client.delete(key=Key("foo"))
    assert result is False


def test_invalidate_cmd(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())

    cache_client.invalidate(key="foo")
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.invalidate(key=Key("foo"))
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.invalidate(key=Key("foo"), cas_token=666)
    memcache_socket.sendall.assert_called_once_with(b"md foo C666\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.invalidate(key=Key("foo"), no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"md foo q\r\n", with_noop=True)
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.invalidate(key=Key("foo"), stale_policy=StalePolicy())
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.invalidate(
        key=Key("foo"),
        stale_policy=StalePolicy(mark_stale_on_deletion_ttl=30),
    )
    memcache_socket.sendall.assert_called_once_with(
        b"md foo I T30\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()


def test_invalidate_success_fail(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())
    result = cache_client.invalidate(key=Key("foo"))
    assert result is True

    memcache_socket.get_response.return_value = Miss()
    result = cache_client.invalidate(key=Key("foo"))
    assert result is True

    memcache_socket.get_response.return_value = NotStored()
    result = cache_client.invalidate(key=Key("foo"))
    assert result is False


def test_touch_cmd(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())

    cache_client.touch(key="foo", ttl=60)
    memcache_socket.sendall.assert_called_once_with(b"mg foo T60\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.touch(key=Key("foo"), ttl=60)
    memcache_socket.sendall.assert_called_once_with(b"mg foo T60\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_client.touch(key=Key("foo"), ttl=60, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo q T60\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()


def assert_called_once_with_command(mock, expected_cmd, **expected_kwargs):
    assert_called_with_commands(mock, [expected_cmd], **expected_kwargs)


def assert_called_with_commands(mock, expected_cmds, **expected_kwargs):
    def split_cmd(cmd: bytes):
        assert cmd.endswith(b"\r\n"), f"Unexpected cmd format: {cmd}"
        pieces = cmd[:-2].split(b" ")
        command = pieces.pop(0)
        key = pieces.pop(0)
        return command, key, set(pieces)

    calls = mock.call_args_list
    assert len(calls) == len(
        expected_cmds
    ), f"Expected exactly {len(expected_cmds)} calls to {mock}"
    for i in range(len(calls)):
        args, kwargs = calls[i]
        expected_cmd = expected_cmds[i]
        assert len(args) == 1, f"Unexpected num of args to {mock}"
        assert (
            kwargs == expected_kwargs
        ), f"Unexpected kwargs to {mock}: {kwargs} expected {expected_kwargs}"
        cmd = args[0]
        actual_cmd, actual_key, actual_flags = split_cmd(cmd)
        expected_cmd, expected_key, expected_flags = split_cmd(expected_cmd)
        assert (
            actual_cmd == expected_cmd
        ), f"Unexpected cmd to {mock}: {actual_cmd} expected {expected_cmd}"
        assert (
            actual_key == expected_key
        ), f"Unexpected key to {mock}: {actual_key} expected {expected_key}"
        assert (
            actual_flags == expected_flags
        ), f"Unexpected flags to {mock}: {actual_flags} expected {expected_flags}"


def test_get_cmd(memcache_socket: MemcacheSocket, cache_client: CacheClient) -> None:
    memcache_socket.get_response.return_value = Miss()

    cache_client.get(key="foo")
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_client.get(key=Key("foo"))
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_client.get(key=Key("foo"), touch_ttl=300)
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v h f T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_client.get(key=Key("foo"), recache_policy=RecachePolicy())
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v h f R30\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_client.get(key=Key("foo"), touch_ttl=300, recache_policy=RecachePolicy())
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v h f R30 T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_client.get(
        key=Key("large_key" * 50), touch_ttl=300, recache_policy=RecachePolicy()
    )
    assert_called_once_with_command(
        memcache_socket.sendall,
        b"mg 4gCNJuSyOJPGW8kRddioRlPx b t l v h f R30 T300\r\n",
        with_noop=False,
    )
    memcache_socket.sendall.reset_mock()

    cache_client.get(
        key=Key("úníçod⍷", is_unicode=True),
        touch_ttl=300,
        recache_policy=RecachePolicy(),
    )
    assert_called_once_with_command(
        memcache_socket.sendall,
        b"mg w7puw63Dp29k4o23 b t l v h f R30 T300\r\n",
        with_noop=False,
    )
    memcache_socket.sendall.reset_mock()

    memcache_socket.get_response.return_value = Value(
        size=0, value=None, flags=ResponseFlags()
    )
    cache_client.get_or_lease(
        key=Key("foo"),
        lease_policy=LeasePolicy(),
        touch_ttl=300,
        recache_policy=RecachePolicy(ttl=60),
    )
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v c h f N30 R60 T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()


def test_get_miss(memcache_socket: MemcacheSocket, cache_client: CacheClient) -> None:
    memcache_socket.get_response.return_value = Miss()

    result, cas_token = cache_client.get_cas_typed(key=Key("foo"), cls=Foo)
    assert result is None
    assert cas_token is None
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v c h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    result = cache_client.get_typed(key=Key("foo"), cls=Foo)
    assert result is None
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    result, cas_token = cache_client.get_cas(key=Key("foo"))
    assert result is None
    assert cas_token is None
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v c h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    result = cache_client.get(key=Key("foo"))
    assert result is None
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()


def test_get_value(memcache_socket: MemcacheSocket, cache_client: CacheClient) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        value=None,
        flags=ResponseFlags(
            cas_token=expected_cas_token,
            client_flag=encoded_value.encoding_id,
        ),
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_client.get_cas_typed(
        key=Key("foo"),
        cls=Foo,
    )
    assert result == expected_value
    assert cas_token == expected_cas_token

    result = cache_client.get_typed(key=Key("foo"), cls=Foo)
    assert result == expected_value

    result, cas_token = cache_client.get_cas(key=Key("foo"))
    assert result == expected_value
    assert cas_token == expected_cas_token

    result = cache_client.get(key=Key("foo"))
    assert result == expected_value


def test_get_other(memcache_socket: MemcacheSocket, cache_client: CacheClient) -> None:
    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())
    try:
        cache_client.get(
            key=Key("foo"),
        )
        raise AssertionError("Should not be reached")
    except MemcacheError as e:
        assert "Unexpected response" in str(e)


def test_value_wrong_type(
    memcache_socket: MemcacheSocket, cache_client: CacheClient
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        value=None,
        flags=ResponseFlags(
            cas_token=expected_cas_token,
            client_flag=encoded_value.encoding_id,
        ),
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_client.get_cas_typed(key=Key("foo"), cls=Bar)
    assert result is None
    assert cas_token == expected_cas_token
    memcache_socket.get_value.assert_called_once_with(len(encoded_value.data))

    try:
        cache_client.get_cas_typed(key=Key("foo"), cls=Bar, error_on_type_mismatch=True)
        raise AssertionError("Should not be reached")
    except ValueError as e:
        assert "Expecting <class 'tests.commands_test.Bar'> got Foo" in str(e)

    result = cache_client.get_typed(key=Key("foo"), cls=Bar)
    assert result is None

    try:
        cache_client.get_typed(key=Key("foo"), cls=Bar, error_on_type_mismatch=True)
        raise AssertionError("Should not be reached")
    except ValueError as e:
        assert "Expecting <class 'tests.commands_test.Bar'> got Foo" in str(e)


def test_deserialization_error(
    memcache_socket: MemcacheSocket, cache_client: CacheClient
) -> None:
    expected_cas_token = 123
    encoded_value = EncodedValue(data=b"invalid", encoding_id=MixedSerializer.PICKLE)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        value=None,
        flags=ResponseFlags(
            cas_token=expected_cas_token,
            client_flag=encoded_value.encoding_id,
        ),
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result = cache_client.get(key=Key("foo"))
    assert result is None


@pytest.fixture
def time(mocker: MockerFixture) -> MagicMock:
    return mocker.patch(
        "meta_memcache.commands.high_level_commands.time", autospec=True
    )


def test_recache_win_returns_miss(
    memcache_socket: MemcacheSocket, cache_client: CacheClient, time: MagicMock
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        value=None,
        flags=ResponseFlags(
            win=True,
            stale=True,
            cas_token=expected_cas_token,
            client_flag=encoded_value.encoding_id,
        ),
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_client.get_cas(key=Key("foo"))
    assert result is None
    assert cas_token == expected_cas_token


def test_recache_lost_returns_stale_value(
    memcache_socket: MemcacheSocket, cache_client: CacheClient, time: MagicMock
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        value=None,
        flags=ResponseFlags(
            win=False,
            stale=True,
            cas_token=expected_cas_token,
            client_flag=encoded_value.encoding_id,
        ),
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_client.get_cas(key=Key("foo"))
    assert result == expected_value
    assert cas_token == expected_cas_token


def test_get_or_lease_hit(
    memcache_socket: MemcacheSocket, cache_client: CacheClient, time: MagicMock
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        value=None,
        flags=ResponseFlags(
            cas_token=expected_cas_token,
            client_flag=encoded_value.encoding_id,
        ),
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result == expected_value
    assert cas_token == expected_cas_token
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v c h f N30\r\n", with_noop=False
    )
    memcache_socket.get_value.assert_called_once_with(len(encoded_value.data))
    time.sleep.assert_not_called()


def test_get_or_lease_miss_win(
    memcache_socket: MemcacheSocket, cache_client: CacheClient, time: MagicMock
) -> None:
    expected_cas_token = 123
    memcache_socket.get_response.return_value = Value(
        size=0,
        value=None,
        flags=ResponseFlags(
            win=True,
            cas_token=expected_cas_token,
        ),
    )
    memcache_socket.get_value.return_value = b""

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result is None
    assert cas_token == expected_cas_token
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v c h f N30\r\n", with_noop=False
    )
    memcache_socket.get_value.assert_called_once_with(0)
    time.sleep.assert_not_called()


def test_get_or_lease_miss_lost_then_data(
    memcache_socket: MemcacheSocket, cache_client: CacheClient, time: MagicMock
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.side_effect = [
        Value(
            size=0,
            value=None,
            flags=ResponseFlags(
                win=False,
                cas_token=expected_cas_token - 1,
            ),
        ),
        Value(
            size=0,
            value=None,
            flags=ResponseFlags(
                win=False,
                cas_token=expected_cas_token - 1,
            ),
        ),
        Value(
            size=len(encoded_value.data),
            value=None,
            flags=ResponseFlags(
                cas_token=expected_cas_token,
                client_flag=encoded_value.encoding_id,
            ),
        ),
    ]
    memcache_socket.get_value.side_effect = [b"", b"", encoded_value.data]

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result == expected_value
    assert cas_token == expected_cas_token
    assert_called_with_commands(
        memcache_socket.sendall,
        [
            b"mg foo t l v c h f N30\r\n",
            b"mg foo t l v c h f N30\r\n",
            b"mg foo t l v c h f N30\r\n",
        ],
        with_noop=False,
    )
    memcache_socket.get_value.assert_has_calls(
        [
            call(0),
            call(0),
            call(len(encoded_value.data)),
        ]
    )
    time.sleep.assert_has_calls([call(1.0), call(1.2)])


def test_get_or_lease_miss_lost_then_win(
    memcache_socket: MemcacheSocket, cache_client: CacheClient, time: MagicMock
) -> None:
    expected_cas_token = 123
    memcache_socket.get_response.side_effect = [
        Value(
            size=0,
            value=None,
            flags=ResponseFlags(
                win=False,
                cas_token=expected_cas_token - 1,
            ),
        ),
        Value(
            size=0,
            value=None,
            flags=ResponseFlags(
                win=False,
                cas_token=expected_cas_token - 1,
            ),
        ),
        Value(
            size=0,
            value=None,
            flags=ResponseFlags(
                win=True,
                cas_token=expected_cas_token,
            ),
        ),
    ]
    memcache_socket.get_value.side_effect = [b"", b"", b""]

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result is None
    assert cas_token == expected_cas_token
    assert_called_with_commands(
        memcache_socket.sendall,
        [
            b"mg foo t l v c h f N30\r\n",
            b"mg foo t l v c h f N30\r\n",
            b"mg foo t l v c h f N30\r\n",
        ],
        with_noop=False,
    )
    memcache_socket.get_value.assert_has_calls(
        [
            call(0),
            call(0),
            call(0),
        ]
    )
    time.sleep.assert_has_calls([call(1.0), call(1.2)])


def test_get_or_lease_miss_runs_out_of_retries(
    memcache_socket: MemcacheSocket, cache_client: CacheClient, time: MagicMock
) -> None:
    expected_cas_token = 123
    memcache_socket.get_response.return_value = Value(
        size=0,
        value=None,
        flags=ResponseFlags(
            win=False,
            cas_token=expected_cas_token,
        ),
    )
    memcache_socket.get_value.return_value = b""

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"),
        lease_policy=LeasePolicy(
            miss_retries=4, wait_backoff_factor=10, miss_max_retry_wait=15
        ),
    )
    assert result is None
    assert cas_token == expected_cas_token
    assert_called_with_commands(
        memcache_socket.sendall,
        [
            b"mg foo t l v c h f N30\r\n",
            b"mg foo t l v c h f N30\r\n",
            b"mg foo t l v c h f N30\r\n",
            b"mg foo t l v c h f N30\r\n",
        ],
        with_noop=False,
    )
    memcache_socket.get_value.assert_has_calls(
        [
            call(0),
            call(0),
            call(0),
            call(0),
        ]
    )
    time.sleep.assert_has_calls([call(1.0), call(10.0), call(15.0)])


def test_get_or_lease_errors(
    memcache_socket: MemcacheSocket, cache_client: CacheClient
) -> None:
    # We should never get a miss
    memcache_socket.get_response.return_value = Miss()
    try:
        cache_client.get_or_lease(
            key=Key("foo"),
            lease_policy=LeasePolicy(),
            touch_ttl=300,
            recache_policy=RecachePolicy(ttl=60),
        )
        raise AssertionError("Should not be reached")
    except MemcacheError:
        pass
    assert_called_once_with_command(
        memcache_socket.sendall, b"mg foo t l v c h f N30 R60 T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    try:
        cache_client.get_or_lease(
            key=Key("foo"),
            lease_policy=LeasePolicy(miss_retries=-10),
            touch_ttl=300,
            recache_policy=RecachePolicy(ttl=60),
        )
        raise AssertionError("Should not be reached")
    except ValueError as e:
        assert "Wrong lease_policy: miss_retries needs to be greater than 0" in str(e)


def test_on_write_failure(
    cache_client: CacheClient,
    connection_pool: ConnectionPool,
) -> None:
    failures_tracked: list[Key] = []
    on_failure: Callable[[Key], None] = lambda key: failures_tracked.append(key)
    cache_client.on_write_failure += on_failure

    connection_pool.pop_connection.side_effect = MemcacheServerError(
        server="broken:11211", message="uh-oh"
    )
    try:
        cache_client.delete(key=Key("foo"))
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 1
    assert failures_tracked[0] == Key("foo")
    failures_tracked.clear()

    try:
        cache_client.set(key=Key("foo"), value=1, ttl=10)
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 1
    assert failures_tracked[0] == Key("foo")
    failures_tracked.clear()


def test_on_write_failure_for_reads(
    cache_client: CacheClient,
    connection_pool: ConnectionPool,
) -> None:
    failures_tracked: list[Key] = []
    on_failure: Callable[[Key], None] = lambda key: failures_tracked.append(key)
    cache_client.on_write_failure += on_failure

    connection_pool.pop_connection.side_effect = MemcacheServerError(
        server="broken:11211", message="uh-oh"
    )
    try:
        cache_client.get(key=Key("foo"))
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 0
    failures_tracked.clear()

    try:
        cache_client.touch(Key("foo"), ttl=3000)
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 0
    failures_tracked.clear()

    try:
        cache_client.touch(Key("foo"), ttl=30)
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 1
    failures_tracked.clear()


def test_on_write_failure_for_multi_ops(
    cache_client: CacheClient,
    connection_pool: ConnectionPool,
) -> None:
    failures_tracked: list[Key] = []
    on_failure: Callable[[Key], None] = lambda key: failures_tracked.append(key)
    cache_client.on_write_failure += on_failure

    connection_pool.pop_connection.side_effect = MemcacheServerError(
        server="broken:11211", message="uh-oh"
    )

    try:
        cache_client.multi_get(keys=[Key("foo"), Key("bar")])
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 0
    failures_tracked.clear()

    try:
        cache_client.multi_get(keys=[Key("foo"), Key("bar")], touch_ttl=30)
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 2
    failures_tracked.clear()


def test_on_write_failure_disabled(
    cache_client: CacheClient,
    connection_pool: ConnectionPool,
) -> None:
    failures_tracked: list[Key] = []
    on_failure: Callable[[Key], None] = lambda key: failures_tracked.append(key)
    cache_client.on_write_failure += on_failure

    connection_pool.pop_connection.side_effect = MemcacheServerError(
        server="broken:11211", message="uh-oh"
    )
    try:
        cache_client.meta_delete(
            key=Key("foo"),
            failure_handling=FailureHandling(track_write_failures=False),
        )
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 0

    try:
        cache_client.meta_set(
            key=Key("foo"),
            value=1,
            ttl=10,
            failure_handling=FailureHandling(track_write_failures=False),
        )
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 0


def test_write_failure_not_raise_on_server_error(
    cache_client_not_raise_on_server_error: CacheClient,
    connection_pool: ConnectionPool,
) -> None:
    cache_client = cache_client_not_raise_on_server_error
    failures_tracked: list[Key] = []
    on_failure: Callable[[Key], None] = lambda key: failures_tracked.append(key)
    cache_client.on_write_failure += on_failure

    connection_pool.pop_connection.side_effect = MemcacheServerError(
        server="broken:11211", message="uh-oh"
    )
    result = cache_client.get(key=Key("foo"))
    assert result is None
    assert len(failures_tracked) == 0
    failures_tracked.clear()

    result = cache_client.delete(key=Key("foo"))
    assert result is False
    assert len(failures_tracked) == 1
    assert failures_tracked[0] == Key("foo")
    failures_tracked.clear()

    result = cache_client.set(key=Key("foo"), value=1, ttl=10)
    assert result is False
    assert len(failures_tracked) == 1
    assert failures_tracked[0] == Key("foo")
    failures_tracked.clear()

    result = cache_client.multi_get(keys=[Key("foo"), Key("bar")])
    assert result == {Key("foo"): None, Key("bar"): None}
    assert len(failures_tracked) == 0
    failures_tracked.clear()


def test_delta_cmd(memcache_socket: MemcacheSocket, cache_client: CacheClient) -> None:
    memcache_socket.get_response.return_value = Miss()

    cache_client.delta(key="foo", delta=1, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"ma foo q D1\r\n", with_noop=True)
    memcache_socket.sendall.reset_mock()

    cache_client.delta(key=Key("foo"), delta=1, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"ma foo q D1\r\n", with_noop=True)
    memcache_socket.sendall.reset_mock()

    cache_client.delta(key=Key("foo"), delta=1, refresh_ttl=60, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(
        b"ma foo q T60 D1\r\n", with_noop=True
    )
    memcache_socket.sendall.reset_mock()

    cache_client.delta(key=Key("foo"), delta=-2, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(
        b"ma foo q D2 M-\r\n", with_noop=True
    )
    memcache_socket.sendall.reset_mock()

    cache_client.delta_initialize(
        key=Key("foo"),
        delta=1,
        initial_value=10,
        initial_ttl=60,
        no_reply=True,
        cas_token=123,
    )
    memcache_socket.sendall.assert_called_once_with(
        b"ma foo q N60 J10 D1 C123\r\n", with_noop=True
    )
    memcache_socket.sendall.reset_mock()

    memcache_socket.get_response.assert_not_called()

    result = cache_client.delta(key=Key("foo"), delta=1)
    assert result is False
    memcache_socket.sendall.assert_called_once_with(b"ma foo D1\r\n", with_noop=False)
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    result = cache_client.delta_and_get(key=Key("foo"), delta=1)
    assert result is None
    memcache_socket.sendall.assert_called_once_with(b"ma foo v D1\r\n", with_noop=False)
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    result = cache_client.delta_initialize_and_get(
        key=Key("foo"), delta=1, initial_value=0, initial_ttl=60
    )
    assert result is None
    memcache_socket.sendall.assert_called_once_with(
        b"ma foo v N60 J0 D1\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    memcache_socket.get_response.return_value = Success(flags=ResponseFlags())

    result = cache_client.delta(key=Key("foo"), delta=1)
    assert result is True
    memcache_socket.sendall.assert_called_once_with(b"ma foo D1\r\n", with_noop=False)
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    memcache_socket.get_response.return_value = Value(
        size=2, value=None, flags=ResponseFlags()
    )
    memcache_socket.get_value.return_value = b"10"

    result = cache_client.delta_and_get(key=Key("foo"), delta=1)
    assert result == 10
    memcache_socket.sendall.assert_called_once_with(b"ma foo v D1\r\n", with_noop=False)
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()
    memcache_socket.get_value.reset_mock()

    result = cache_client.delta_initialize_and_get(
        key=Key("foo"), delta=1, initial_value=0, initial_ttl=60
    )
    assert result == 10
    memcache_socket.sendall.assert_called_once_with(
        b"ma foo v N60 J0 D1\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()


def test_multi_get(memcache_socket: MemcacheSocket, cache_client: CacheClient) -> None:
    memcache_socket.get_response.side_effect = [
        Miss(),
        Value(
            size=2,
            value=None,
            flags=ResponseFlags(client_flag=MixedSerializer.BINARY),
        ),
        Value(
            size=2,
            value=None,
            flags=ResponseFlags(win=True),
        ),
    ]
    memcache_socket.get_value.return_value = b"OK"

    results = cache_client.multi_get(
        keys=[
            Key("miss"),
            Key("found"),
            Key("lease"),
        ]
    )
    assert_called_with_commands(
        memcache_socket.sendall,
        [
            b"mg miss t l v h f\r\n",
            b"mg found t l v h f\r\n",
            b"mg lease t l v h f\r\n",
        ],
        with_noop=False,
    )
    assert memcache_socket.get_response.call_count == 3
    assert memcache_socket.get_value.mock_calls == [
        call(2),
        call(2),
    ]
    assert results == {
        Key("miss"): None,
        Key("found"): b"OK",
        Key("lease"): None,
    }
