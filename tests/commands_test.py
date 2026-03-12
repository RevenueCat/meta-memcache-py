import base64
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

from tests.conftest import WireSocket

# Response constants (re-exported from conftest for convenience)
EN = b"EN\r\n"


@dataclass
class Foo:
    bar: str


@dataclass
class Bar:
    foo: str


# ---------------------------------------------------------------------------
# Mock-based fixtures (used by tests that check return-value semantics)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Wire-based fixtures (used by tests that check the actual bytes on the wire)
# ---------------------------------------------------------------------------


@pytest.fixture
def wire_connection_pool(
    mocker: MockerFixture, wire_memcache_socket: WireSocket
) -> ConnectionPool:
    pool = mocker.MagicMock(spec=ConnectionPool)
    pool.pop_connection.return_value = wire_memcache_socket.memcache_socket
    return pool


@pytest.fixture
def wire_connection_pool_1_6_6(
    mocker: MockerFixture, wire_memcache_socket_1_6_6: WireSocket
) -> ConnectionPool:
    pool = mocker.MagicMock(spec=ConnectionPool)
    pool.pop_connection.return_value = wire_memcache_socket_1_6_6.memcache_socket
    return pool


@pytest.fixture
def wire_cache_client(wire_connection_pool: ConnectionPool) -> CacheClient:
    return get_test_cache_client(wire_connection_pool)


@pytest.fixture
def wire_cache_client_1_6_6(
    wire_connection_pool_1_6_6: ConnectionPool,
) -> CacheClient:
    return get_test_cache_client(wire_connection_pool_1_6_6)


# ===================================================================
# Wire-format tests
# ===================================================================


def test_set_cmd(
    wire_memcache_socket: WireSocket,
    wire_cache_client: CacheClient,
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client

    # str key
    cache_client.set(key="foo", value="bar", ttl=300)
    ws.assert_wire(b"ms foo 3 T300 F0\r\nbar\r\n")

    # Key object
    cache_client.set(key=Key("foo"), value="bar", ttl=300)
    ws.assert_wire(b"ms foo 3 T300 F0\r\nbar\r\n")

    # int value -> F2
    cache_client.set(key=Key("foo"), value=123, ttl=300)
    ws.assert_wire(b"ms foo 3 T300 F2\r\n123\r\n")

    # list value -> pickled, F1
    value_list = [1, 2, 3]
    data = pickle.dumps(value_list, protocol=0)
    cache_client.set(key=Key("foo"), value=value_list, ttl=300)
    ws.assert_wire(
        b"ms foo " + str(len(data)).encode() + b" T300 F1\r\n" + data + b"\r\n"
    )

    # bool value -> pickled, F1
    value_bool = False
    data = pickle.dumps(value_bool, protocol=0)
    cache_client.set(key=Key("foo"), value=value_bool, ttl=300)
    ws.assert_wire(
        b"ms foo " + str(len(data)).encode() + b" T300 F1\r\n" + data + b"\r\n"
    )

    # bytes value -> F16 (BINARY)
    cache_client.set(key=Key("foo"), value=b"123", ttl=300)
    ws.assert_wire(b"ms foo 3 T300 F16\r\n123\r\n")

    # large bytes -> compressed, F24 (BINARY|ZLIB_COMPRESSED)
    value_bytes = b"123" * 100
    data = zlib.compress(value_bytes)
    cache_client.set(key=Key("foo"), value=value_bytes, ttl=300)
    ws.assert_wire(
        b"ms foo " + str(len(data)).encode() + b" T300 F24\r\n" + data + b"\r\n"
    )

    # SetMode.ADD -> ME
    cache_client.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.ADD)
    ws.assert_wire(b"ms foo 3 T300 F2 ME\r\n123\r\n")

    # SetMode.APPEND -> MA
    cache_client.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.APPEND)
    ws.assert_wire(b"ms foo 3 T300 F2 MA\r\n123\r\n")

    # SetMode.PREPEND -> MP
    cache_client.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.PREPEND)
    ws.assert_wire(b"ms foo 3 T300 F2 MP\r\n123\r\n")

    # SetMode.REPLACE -> MR
    cache_client.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.REPLACE)
    ws.assert_wire(b"ms foo 3 T300 F2 MR\r\n123\r\n")

    # no_reply -> q flag + mn noop
    cache_client.set(key=Key("foo"), value=b"123", ttl=300, no_reply=True)
    ws.assert_wire(b"ms foo 3 q T300 F16\r\n123\r\nmn\r\n")

    # cas_token -> C666
    cache_client.set(key=Key("foo"), value=b"123", ttl=300, cas_token=666)
    ws.assert_wire(b"ms foo 3 T300 F16 C666\r\n123\r\n")

    # cas_token + stale_policy (no mark_stale_on_cas_mismatch) -> no I flag
    cache_client.set(
        key=Key("foo"), value=b"123", ttl=300, cas_token=666, stale_policy=StalePolicy()
    )
    ws.assert_wire(b"ms foo 3 T300 F16 C666\r\n123\r\n")

    # cas_token + stale_policy(mark_stale_on_cas_mismatch=True) -> I flag
    cache_client.set(
        key=Key("foo"),
        value=b"123",
        ttl=300,
        cas_token=666,
        stale_policy=StalePolicy(mark_stale_on_cas_mismatch=True),
    )
    ws.assert_wire(b"ms foo 3 I T300 F16 C666\r\n123\r\n")


def test_set_cmd_1_6_6(
    wire_memcache_socket_1_6_6: WireSocket,
    wire_cache_client_1_6_6: CacheClient,
) -> None:
    ws = wire_memcache_socket_1_6_6
    cache_client = wire_cache_client_1_6_6

    # 1.6.6 uses S-prefixed size: S3 instead of 3
    cache_client.set(key="foo", value="bar", ttl=300)
    ws.assert_wire(b"ms foo S3 T300 F0\r\nbar\r\n")


def test_set_success_fail(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.meta_set.return_value = Success(flags=ResponseFlags())
    result = cache_client.set(key=Key("foo"), value="bar", ttl=300)
    assert result is True

    memcache_socket.meta_set.return_value = NotStored()
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
    wire_memcache_socket: WireSocket,
    wire_cache_client: CacheClient,
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client

    # str key
    cache_client.delete(key="foo")
    ws.assert_wire(b"md foo\r\n")

    # Key object
    cache_client.delete(key=Key("foo"))
    ws.assert_wire(b"md foo\r\n")

    # cas_token
    cache_client.delete(key=Key("foo"), cas_token=666)
    ws.assert_wire(b"md foo C666\r\n")

    # no_reply
    cache_client.delete(key=Key("foo"), no_reply=True)
    ws.assert_wire(b"md foo q\r\nmn\r\n")

    # stale_policy with default (mark_stale_on_deletion_ttl=0) -> no flags
    cache_client.delete(key=Key("foo"), stale_policy=StalePolicy())
    ws.assert_wire(b"md foo\r\n")

    # stale_policy with mark_stale_on_deletion_ttl=30 -> I T30
    cache_client.delete(
        key=Key("foo"),
        stale_policy=StalePolicy(mark_stale_on_deletion_ttl=30),
    )
    ws.assert_wire(b"md foo I T30\r\n")


def test_delete_success_fail(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.meta_delete.return_value = Success(flags=ResponseFlags())
    result = cache_client.delete(key=Key("foo"))
    assert result is True

    memcache_socket.meta_delete.return_value = Miss()
    result = cache_client.delete(key=Key("foo"))
    assert result is False

    memcache_socket.meta_delete.return_value = NotStored()
    result = cache_client.delete(key=Key("foo"))
    assert result is False


def test_invalidate_cmd(
    wire_memcache_socket: WireSocket,
    wire_cache_client: CacheClient,
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client

    # str key (invalidate sends plain md, no flags since stale_policy is None)
    cache_client.invalidate(key="foo")
    ws.assert_wire(b"md foo\r\n")

    # Key object
    cache_client.invalidate(key=Key("foo"))
    ws.assert_wire(b"md foo\r\n")

    # cas_token
    cache_client.invalidate(key=Key("foo"), cas_token=666)
    ws.assert_wire(b"md foo C666\r\n")

    # no_reply
    cache_client.invalidate(key=Key("foo"), no_reply=True)
    ws.assert_wire(b"md foo q\r\nmn\r\n")

    # stale_policy with default -> no flags added
    cache_client.invalidate(key=Key("foo"), stale_policy=StalePolicy())
    ws.assert_wire(b"md foo\r\n")

    # stale_policy with mark_stale_on_deletion_ttl=30 -> I T30
    cache_client.invalidate(
        key=Key("foo"),
        stale_policy=StalePolicy(mark_stale_on_deletion_ttl=30),
    )
    ws.assert_wire(b"md foo I T30\r\n")


def test_invalidate_success_fail(
    memcache_socket: MemcacheSocket,
    cache_client: CacheClient,
) -> None:
    memcache_socket.meta_delete.return_value = Success(flags=ResponseFlags())
    result = cache_client.invalidate(key=Key("foo"))
    assert result is True

    memcache_socket.meta_delete.return_value = Miss()
    result = cache_client.invalidate(key=Key("foo"))
    assert result is True

    memcache_socket.meta_delete.return_value = NotStored()
    result = cache_client.invalidate(key=Key("foo"))
    assert result is False


def test_touch_cmd(
    wire_memcache_socket: WireSocket,
    wire_cache_client: CacheClient,
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client

    # touch uses mg with TTL flag
    cache_client.touch(key="foo", ttl=60)
    ws.assert_wire(b"mg foo T60\r\n")

    cache_client.touch(key=Key("foo"), ttl=60)
    ws.assert_wire(b"mg foo T60\r\n")

    cache_client.touch(key=Key("foo"), ttl=60, no_reply=True)
    ws.assert_wire(b"mg foo q T60\r\n")


def test_get_cmd(
    wire_memcache_socket: WireSocket,
    wire_cache_client: CacheClient,
) -> None:
    ws = wire_memcache_socket
    ws.default_response = EN  # Return miss for all gets
    cache_client = wire_cache_client

    # Default get flags: f v t l h (return_client_flag, return_value, return_ttl,
    # return_last_access, return_fetched)
    cache_client.get(key="foo")
    ws.assert_wire(b"mg foo f v t l h\r\n")

    cache_client.get(key=Key("foo"))
    ws.assert_wire(b"mg foo f v t l h\r\n")

    # touch_ttl adds T300
    cache_client.get(key=Key("foo"), touch_ttl=300)
    ws.assert_wire(b"mg foo f v t l h T300\r\n")

    # recache_policy adds R30 (default RecachePolicy.ttl=30)
    cache_client.get(key=Key("foo"), recache_policy=RecachePolicy())
    ws.assert_wire(b"mg foo f v t l h R30\r\n")

    # touch_ttl + recache_policy
    cache_client.get(key=Key("foo"), touch_ttl=300, recache_policy=RecachePolicy())
    ws.assert_wire(b"mg foo f v t l h T300 R30\r\n")

    # large key gets hashed; the raw bytes from default_key_encoder are
    # base64-encoded in the wire protocol
    large_key = Key("large_key" * 50)
    encoded_key_bytes = default_key_encoder(large_key)
    wire_key = base64.b64encode(encoded_key_bytes, altchars=b"-_").rstrip(b"=")
    cache_client.get(key=large_key, touch_ttl=300, recache_policy=RecachePolicy())
    ws.assert_wire(b"mg " + wire_key + b" b f v t l h T300 R30\r\n")

    # unicode key is base64-encoded on the wire (non-ASCII bytes)
    unicode_key = Key("\u00fan\u00ed\u00e7od\u2377")
    encoded_unicode = default_key_encoder(unicode_key)
    wire_unicode_key = base64.b64encode(encoded_unicode, altchars=b"-_").rstrip(b"=")
    cache_client.get(key=unicode_key, touch_ttl=300, recache_policy=RecachePolicy())
    ws.assert_wire(b"mg " + wire_unicode_key + b" b f v t l h T300 R30\r\n")

    # get_or_lease adds c (return_cas_token), N30 (vivify_on_miss_ttl=30),
    # R60 (recache_ttl=60), T300 (cache_ttl=300)
    ws.queue_response(b"VA 0 W\r\n\r\n")
    cache_client.get_or_lease(
        key=Key("foo"),
        lease_policy=LeasePolicy(),
        touch_ttl=300,
        recache_policy=RecachePolicy(ttl=60),
    )
    ws.assert_wire(b"mg foo f c v t l h T300 R60 N30\r\n")


def test_get_miss(
    wire_memcache_socket: WireSocket, wire_cache_client: CacheClient
) -> None:
    ws = wire_memcache_socket
    ws.default_response = b"EN\r\n"
    cache_client = wire_cache_client

    result, cas_token = cache_client.get_cas_typed(key=Key("foo"), cls=Foo)
    assert result is None
    assert cas_token is None
    ws.assert_wire(b"mg foo f c v t l h\r\n")

    result = cache_client.get_typed(key=Key("foo"), cls=Foo)
    assert result is None
    ws.assert_wire(b"mg foo f v t l h\r\n")

    result, cas_token = cache_client.get_cas(key=Key("foo"))
    assert result is None
    assert cas_token is None
    ws.assert_wire(b"mg foo f c v t l h\r\n")

    result = cache_client.get(key=Key("foo"))
    assert result is None
    ws.assert_wire(b"mg foo f v t l h\r\n")


def test_get_value(memcache_socket: MemcacheSocket, cache_client: CacheClient) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(Key("foo"), expected_value)

    def _make_value():
        return Value(
            size=len(encoded_value.data),
            value=encoded_value.data,
            flags=ResponseFlags(
                cas_token=expected_cas_token,
                client_flag=encoded_value.encoding_id,
            ),
        )

    memcache_socket.meta_get.side_effect = lambda *a, **kw: _make_value()

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
    memcache_socket.meta_get.return_value = Success(flags=ResponseFlags())
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
    encoded_value = MixedSerializer().serialize(Key("foo"), expected_value)

    def _make_value():
        return Value(
            size=len(encoded_value.data),
            value=encoded_value.data,
            flags=ResponseFlags(
                cas_token=expected_cas_token,
                client_flag=encoded_value.encoding_id,
            ),
        )

    memcache_socket.meta_get.side_effect = lambda *a, **kw: _make_value()
    result, cas_token = cache_client.get_cas_typed(key=Key("foo"), cls=Bar)
    assert result is None
    assert cas_token == expected_cas_token

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
    memcache_socket.meta_get.return_value = Value(
        size=len(encoded_value.data),
        value=encoded_value.data,
        flags=ResponseFlags(
            cas_token=expected_cas_token,
            client_flag=encoded_value.encoding_id,
        ),
    )

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
    encoded_value = MixedSerializer().serialize(Key("foo"), expected_value)
    memcache_socket.meta_get.return_value = Value(
        size=len(encoded_value.data),
        value=encoded_value.data,
        flags=ResponseFlags(
            win=True,
            stale=True,
            cas_token=expected_cas_token,
            client_flag=encoded_value.encoding_id,
        ),
    )

    result, cas_token = cache_client.get_cas(key=Key("foo"))
    assert result is None
    assert cas_token == expected_cas_token


def test_recache_lost_returns_stale_value(
    memcache_socket: MemcacheSocket, cache_client: CacheClient, time: MagicMock
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(Key("foo"), expected_value)
    memcache_socket.meta_get.return_value = Value(
        size=len(encoded_value.data),
        value=encoded_value.data,
        flags=ResponseFlags(
            win=False,
            stale=True,
            cas_token=expected_cas_token,
            client_flag=encoded_value.encoding_id,
        ),
    )

    result, cas_token = cache_client.get_cas(key=Key("foo"))
    assert result == expected_value
    assert cas_token == expected_cas_token


def test_get_or_lease_hit(
    wire_memcache_socket: WireSocket, wire_cache_client: CacheClient, time: MagicMock
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client

    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(Key("foo"), expected_value)

    ws.queue_response(
        b"VA "
        + str(len(encoded_value.data)).encode()
        + b" c123 f"
        + str(encoded_value.encoding_id).encode()
        + b"\r\n"
        + encoded_value.data
        + b"\r\n"
    )

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result == expected_value
    assert cas_token == expected_cas_token
    ws.assert_wire(b"mg foo f c v t l h N30\r\n")
    time.sleep.assert_not_called()


def test_get_or_lease_miss_win(
    wire_memcache_socket: WireSocket, wire_cache_client: CacheClient, time: MagicMock
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client
    expected_cas_token = 123
    ws.queue_response(b"VA 0 W c123\r\n\r\n")

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result is None
    assert cas_token == expected_cas_token
    time.sleep.assert_not_called()
    ws.assert_wire(b"mg foo f c v t l h N30\r\n")


def test_get_or_lease_miss_lost_then_data(
    wire_memcache_socket: WireSocket, wire_cache_client: CacheClient, time: MagicMock
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(Key("foo"), expected_value)
    data = encoded_value.data
    eid = encoded_value.encoding_id
    # Two miss-lost responses (size=0, Z=loss), then a hit with data
    ws.queue_response(
        b"VA 0 Z c122\r\n\r\n",
        b"VA 0 Z c122\r\n\r\n",
        b"VA "
        + str(len(data)).encode()
        + b" c123 f"
        + str(eid).encode()
        + b"\r\n"
        + data
        + b"\r\n",
    )

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result == expected_value
    assert cas_token == expected_cas_token
    time.sleep.assert_has_calls([call(1.0), call(1.2)])
    ws.assert_wire(
        b"mg foo f c v t l h N30\r\n",
        b"mg foo f c v t l h N30\r\n",
        b"mg foo f c v t l h N30\r\n",
    )


def test_get_or_lease_miss_lost_then_win(
    wire_memcache_socket: WireSocket, wire_cache_client: CacheClient, time: MagicMock
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client
    expected_cas_token = 123
    # Two miss-lost (Z=loss), then a miss-win (W)
    ws.queue_response(
        b"VA 0 Z c122\r\n\r\n",
        b"VA 0 Z c122\r\n\r\n",
        b"VA 0 W c123\r\n\r\n",
    )

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result is None
    assert cas_token == expected_cas_token
    time.sleep.assert_has_calls([call(1.0), call(1.2)])
    ws.assert_wire(
        b"mg foo f c v t l h N30\r\n",
        b"mg foo f c v t l h N30\r\n",
        b"mg foo f c v t l h N30\r\n",
    )


def test_get_or_lease_miss_runs_out_of_retries(
    wire_memcache_socket: WireSocket, wire_cache_client: CacheClient, time: MagicMock
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client
    expected_cas_token = 123
    # All responses are miss-lost (Z=loss); default_response won't be used
    ws.queue_response(
        b"VA 0 Z c123\r\n\r\n",
        b"VA 0 Z c123\r\n\r\n",
        b"VA 0 Z c123\r\n\r\n",
        b"VA 0 Z c123\r\n\r\n",
    )

    result, cas_token = cache_client.get_or_lease_cas(
        key=Key("foo"),
        lease_policy=LeasePolicy(
            miss_retries=4, wait_backoff_factor=10, miss_max_retry_wait=15
        ),
    )
    assert result is None
    assert cas_token == expected_cas_token
    time.sleep.assert_has_calls([call(1.0), call(10.0), call(15.0)])
    ws.assert_wire(
        b"mg foo f c v t l h N30\r\n",
        b"mg foo f c v t l h N30\r\n",
        b"mg foo f c v t l h N30\r\n",
        b"mg foo f c v t l h N30\r\n",
    )


def test_get_or_lease_errors(
    wire_memcache_socket: WireSocket, wire_cache_client: CacheClient
) -> None:
    ws = wire_memcache_socket
    ws.default_response = b"EN\r\n"
    cache_client = wire_cache_client

    # We should never get a miss from get_or_lease (it uses N flag for vivify)
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
    ws.assert_wire(b"mg foo f c v t l h T300 R60 N30\r\n")

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


def test_delta_cmd(
    wire_memcache_socket: WireSocket,
    wire_cache_client: CacheClient,
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client

    # delta +1, no_reply
    cache_client.delta(key="foo", delta=1, no_reply=True)
    ws.assert_wire(b"ma foo q D1\r\nmn\r\n")

    cache_client.delta(key=Key("foo"), delta=1, no_reply=True)
    ws.assert_wire(b"ma foo q D1\r\nmn\r\n")

    # delta +1 with refresh_ttl, no_reply
    cache_client.delta(key=Key("foo"), delta=1, refresh_ttl=60, no_reply=True)
    ws.assert_wire(b"ma foo q T60 D1\r\nmn\r\n")

    # delta -2, no_reply -> D2 M-
    cache_client.delta(key=Key("foo"), delta=-2, no_reply=True)
    ws.assert_wire(b"ma foo q D2 M-\r\nmn\r\n")

    # delta_initialize with all flags
    cache_client.delta_initialize(
        key=Key("foo"),
        delta=1,
        initial_value=10,
        initial_ttl=60,
        no_reply=True,
        cas_token=123,
    )
    ws.assert_wire(b"ma foo q N60 J10 D1 C123\r\nmn\r\n")

    # delta +1 (blocking, default HD response -> Success -> True)
    result = cache_client.delta(key=Key("foo"), delta=1)
    assert result is True
    ws.assert_wire(b"ma foo D1\r\n")

    # delta_and_get -> returns value
    ws.queue_response(b"VA 2\r\n10\r\n")
    result = cache_client.delta_and_get(key=Key("foo"), delta=1)
    assert result == 10
    ws.assert_wire(b"ma foo v D1\r\n")

    # delta_initialize_and_get
    ws.queue_response(b"VA 2\r\n10\r\n")
    result = cache_client.delta_initialize_and_get(
        key=Key("foo"), delta=1, initial_value=0, initial_ttl=60
    )
    assert result == 10
    ws.assert_wire(b"ma foo v N60 J0 D1\r\n")


def test_multi_get(
    wire_memcache_socket: WireSocket,
    wire_cache_client: CacheClient,
) -> None:
    ws = wire_memcache_socket
    cache_client = wire_cache_client

    # Queue responses for pipelined gets
    ws.queue_response(EN)  # miss
    ws.queue_response(b"VA 2 f16\r\nOK\r\n")  # Value with client_flag=BINARY(16)
    ws.queue_response(b"VA 2 W\r\nOK\r\n")  # Value with win=True (lease)

    results = cache_client.multi_get(
        keys=[
            Key("miss"),
            Key("found"),
            Key("lease"),
        ]
    )

    wire = ws.read_wire()
    # All three gets should be pipelined with default get flags
    assert b"mg miss" in wire
    assert b"mg found" in wire
    assert b"mg lease" in wire
    # Check the full wire: three mg commands, each with f v t l h flags
    lines = [line for line in wire.split(b"\r\n") if line]
    assert len(lines) == 3
    for line in lines:
        assert line.startswith(b"mg ")
        parts = set(line.split(b" ")[2:])
        assert {b"f", b"v", b"t", b"l", b"h"} == parts

    assert results == {
        Key("miss"): None,
        Key("found"): b"OK",
        Key("lease"): None,
    }


def test_multi_get_with_values(
    memcache_socket: MemcacheSocket, cache_client: CacheClient
) -> None:
    """Test multi_get with deserialized values of various types."""
    serializer = MixedSerializer()
    str_encoded = serializer.serialize(Key("k1"), "hello")
    int_encoded = serializer.serialize(Key("k2"), 42)
    list_encoded = serializer.serialize(Key("k3"), [1, 2])

    memcache_socket.get_response.side_effect = [
        Value(
            size=len(str_encoded.data),
            value=str_encoded.data,
            flags=ResponseFlags(client_flag=str_encoded.encoding_id),
        ),
        Value(
            size=len(int_encoded.data),
            value=int_encoded.data,
            flags=ResponseFlags(client_flag=int_encoded.encoding_id),
        ),
        Value(
            size=len(list_encoded.data),
            value=list_encoded.data,
            flags=ResponseFlags(client_flag=list_encoded.encoding_id),
        ),
    ]

    results = cache_client.multi_get(keys=[Key("k1"), Key("k2"), Key("k3")])

    calls = memcache_socket.send_meta_get.call_args_list
    assert len(calls) == 3
    assert calls[0][0][0] == b"k1"
    assert calls[1][0][0] == b"k2"
    assert calls[2][0][0] == b"k3"
    assert memcache_socket.get_response.call_count == 3
    assert results == {
        Key("k1"): "hello",
        Key("k2"): 42,
        Key("k3"): [1, 2],
    }


def test_multi_get_with_touch_ttl(
    memcache_socket: MemcacheSocket, cache_client: CacheClient
) -> None:
    """Test multi_get passes touch_ttl through to the pipelining path."""
    memcache_socket.get_response.side_effect = [
        Miss(),
        Miss(),
    ]

    results = cache_client.multi_get(
        keys=[Key("a"), Key("b")],
        touch_ttl=300,
    )

    calls = memcache_socket.send_meta_get.call_args_list
    assert len(calls) == 2
    assert calls[0][0][0] == b"a"
    assert calls[1][0][0] == b"b"
    # Verify the flags contain the touch TTL
    for c in calls:
        flags = c[0][1]
        assert flags is not None
        assert flags.cache_ttl == 300
    assert results == {Key("a"): None, Key("b"): None}


def test_multi_get_with_recache(
    memcache_socket: MemcacheSocket, cache_client: CacheClient
) -> None:
    """Test multi_get with recache_policy passes recache_ttl in flags."""
    memcache_socket.get_response.side_effect = [Miss()]

    cache_client.multi_get(
        keys=[Key("x")],
        recache_policy=RecachePolicy(ttl=60),
    )

    calls = memcache_socket.send_meta_get.call_args_list
    assert len(calls) == 1
    flags = calls[0][0][1]
    assert flags is not None
    assert flags.recache_ttl == 60


def test_meta_multiget_no_reply(
    memcache_socket: MemcacheSocket, cache_client: CacheClient
) -> None:
    """Test meta_multiget with no_reply flag uses pipelining path correctly.

    With no_reply on mg, the executor skips get_response and returns Success.
    """
    results = cache_client.meta_multiget(
        keys=[Key("a"), Key("b")],
        flags=RequestFlags(no_reply=True, cache_ttl=60),
    )

    calls = memcache_socket.send_meta_get.call_args_list
    assert len(calls) == 2
    assert calls[0][0][0] == b"a"
    assert calls[1][0][0] == b"b"
    # no_reply: get_response not called, executor returns Success directly
    memcache_socket.get_response.assert_not_called()
    assert all(isinstance(r, Success) for r in results.values())
