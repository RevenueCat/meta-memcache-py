import pickle
from typing import Callable
import zlib
from dataclasses import dataclass
from unittest.mock import MagicMock, call

import pytest
from pytest_mock import MockerFixture

from meta_memcache import CachePool, Key, MemcacheError, SetMode
from meta_memcache.base.connection_pool import ConnectionPool
from meta_memcache.base.memcache_socket import MemcacheSocket
from meta_memcache.configuration import (
    LeasePolicy,
    RecachePolicy,
    StalePolicy,
    default_binary_key_encoding,
)
from meta_memcache.errors import MemcacheServerError
from meta_memcache.protocol import (
    Flag,
    IntFlag,
    Miss,
    NotStored,
    ServerVersion,
    Success,
    Value,
)
from meta_memcache.serializer import MixedSerializer


class FakeCachePool(CachePool):
    def __init__(self, connection_pool: ConnectionPool, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.connection_pool = connection_pool

    def _get_pool(self, key: Key) -> ConnectionPool:
        return self.connection_pool


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
def cache_pool(
    mocker: MockerFixture,
    memcache_socket: MemcacheSocket,
) -> FakeCachePool:
    connection_pool = mocker.MagicMock(spec=ConnectionPool)
    connection_pool.get_connection().__enter__.return_value = memcache_socket
    return FakeCachePool(
        connection_pool=connection_pool,
        serializer=MixedSerializer(),
        binary_key_encoding_fn=default_binary_key_encoding,
    )


@pytest.fixture
def cache_pool_not_raise_on_server_error(
    mocker: MockerFixture,
    memcache_socket: MemcacheSocket,
) -> FakeCachePool:
    connection_pool = mocker.MagicMock(spec=ConnectionPool)
    connection_pool.get_connection().__enter__.return_value = memcache_socket
    return FakeCachePool(
        connection_pool=connection_pool,
        serializer=MixedSerializer(),
        binary_key_encoding_fn=default_binary_key_encoding,
        raise_on_server_error=False,
    )


@pytest.fixture
def cache_pool_1_6_6(
    mocker: MockerFixture,
    memcache_socket_1_6_6: MemcacheSocket,
) -> FakeCachePool:
    connection_pool = mocker.MagicMock(spec=ConnectionPool)
    connection_pool.get_connection().__enter__.return_value = memcache_socket_1_6_6
    return FakeCachePool(
        connection_pool=connection_pool,
        serializer=MixedSerializer(),
        binary_key_encoding_fn=default_binary_key_encoding,
    )


def test_set_cmd(
    memcache_socket: MemcacheSocket,
    cache_pool: FakeCachePool,
) -> None:
    memcache_socket.get_response.return_value = Success()

    cache_pool.set(key="foo", value="bar", ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F0\r\nbar\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value="bar", ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F0\r\nbar\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=123, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    value = [1, 2, 3]
    data = pickle.dumps(value, protocol=0)
    cache_pool.set(key=Key("foo"), value=value, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo " + str(len(data)).encode() + b" T300 F1\r\n" + data + b"\r\n",
        with_noop=False,
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    value = False  # Bools should be stored pickled
    data = pickle.dumps(value, protocol=0)
    cache_pool.set(key=Key("foo"), value=value, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo " + str(len(data)).encode() + b" T300 F1\r\n" + data + b"\r\n",
        with_noop=False,
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=b"123", ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F16\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    value = b"123" * 100
    data = zlib.compress(value)
    cache_pool.set(key=Key("foo"), value=value, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo " + str(len(data)).encode() + b" T300 F24\r\n" + data + b"\r\n",
        with_noop=False,
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.ADD)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2 ME\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.APPEND)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2 MA\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.PREPEND)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2 MP\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=123, ttl=300, set_mode=SetMode.REPLACE)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 F2 MR\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=b"123", ttl=300, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 q T300 F16\r\n123\r\n", with_noop=True
    )
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=b"123", ttl=300, cas_token=666)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 C666 F16\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(
        key=Key("foo"), value=b"123", ttl=300, cas_token=666, stale_policy=StalePolicy()
    )
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 T300 C666 F16\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(
        key=Key("foo"),
        value=b"123",
        ttl=300,
        cas_token=666,
        stale_policy=StalePolicy(mark_stale_on_cas_mismatch=True),
    )
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 I T300 C666 F16\r\n123\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()


def test_set_cmd_1_6_6(
    memcache_socket_1_6_6: MemcacheSocket,
    cache_pool_1_6_6: FakeCachePool,
) -> None:
    memcache_socket_1_6_6.get_response.return_value = Success()

    cache_pool_1_6_6.set(key="foo", value="bar", ttl=300)
    memcache_socket_1_6_6.sendall.assert_called_once_with(
        b"ms foo S3 T300 F0\r\nbar\r\n", with_noop=False
    )
    memcache_socket_1_6_6.get_response.assert_called_once_with()


def test_set_success_fail(
    memcache_socket: MemcacheSocket,
    cache_pool: FakeCachePool,
) -> None:
    memcache_socket.get_response.return_value = Success()
    result = cache_pool.set(key=Key("foo"), value="bar", ttl=300)
    assert result is True

    memcache_socket.get_response.return_value = NotStored()
    result = cache_pool.set(key=Key("foo"), value="bar", ttl=300)
    assert result is False


def test_delete_cmd(
    memcache_socket: MemcacheSocket,
    cache_pool: FakeCachePool,
) -> None:
    memcache_socket.get_response.return_value = Success()

    cache_pool.delete(key="foo")
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.delete(key=Key("foo"))
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.delete(key=Key("foo"), cas_token=666)
    memcache_socket.sendall.assert_called_once_with(b"md foo C666\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.delete(key=Key("foo"), no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"md foo q\r\n", with_noop=True)
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.delete(key=Key("foo"), stale_policy=StalePolicy())
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.delete(
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
    cache_pool: FakeCachePool,
) -> None:
    memcache_socket.get_response.return_value = Success()
    result = cache_pool.delete(key=Key("foo"))
    assert result is True

    memcache_socket.get_response.return_value = NotStored()
    result = cache_pool.delete(key=Key("foo"))
    assert result is False


def test_touch_cmd(
    memcache_socket: MemcacheSocket,
    cache_pool: FakeCachePool,
) -> None:
    memcache_socket.get_response.return_value = Success()

    cache_pool.touch(key="foo", ttl=60)
    memcache_socket.sendall.assert_called_once_with(b"mg foo T60\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.touch(key=Key("foo"), ttl=60)
    memcache_socket.sendall.assert_called_once_with(b"mg foo T60\r\n", with_noop=False)
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.touch(key=Key("foo"), ttl=60, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo q T60\r\n", with_noop=False
    )
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()


def test_get_cmd(memcache_socket: MemcacheSocket, cache_pool: FakeCachePool) -> None:
    memcache_socket.get_response.return_value = Miss()

    cache_pool.get(key="foo")
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_pool.get(key=Key("foo"))
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_pool.get(key=Key("foo"), touch_ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v h f T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_pool.get(key=Key("foo"), recache_policy=RecachePolicy())
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v h f R30\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_pool.get(key=Key("foo"), touch_ttl=300, recache_policy=RecachePolicy())
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v h f R30 T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_pool.get(
        key=Key("large_key" * 50), touch_ttl=300, recache_policy=RecachePolicy()
    )
    memcache_socket.sendall.assert_called_once_with(
        b"mg 4gCNJuSyOJPGW8kRddioRlPx b t l v h f R30 T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    cache_pool.get(
        key=Key("úníçod⍷", is_unicode=True),
        touch_ttl=300,
        recache_policy=RecachePolicy(),
    )
    memcache_socket.sendall.assert_called_once_with(
        b"mg lCV3WxKxtWrdY4s1+R710+9J b t l v h f R30 T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    memcache_socket.get_response.return_value = Value(size=0)
    cache_pool.get_or_lease(
        key=Key("foo"),
        lease_policy=LeasePolicy(),
        touch_ttl=300,
        recache_policy=RecachePolicy(ttl=60),
    )
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v c h f N30 R60 T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()


def test_get_miss(memcache_socket: MemcacheSocket, cache_pool: FakeCachePool) -> None:
    memcache_socket.get_response.return_value = Miss()

    result, cas_token = cache_pool.get_cas_typed(key=Key("foo"), cls=Foo)
    assert result is None
    assert cas_token is None
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v c h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    result = cache_pool.get_typed(key=Key("foo"), cls=Foo)
    assert result is None
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    result, cas_token = cache_pool.get_cas(key=Key("foo"))
    assert result is None
    assert cas_token is None
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v c h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    result = cache_pool.get(key=Key("foo"))
    assert result is None
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v h f\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()


def test_get_value(memcache_socket: MemcacheSocket, cache_pool: FakeCachePool) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.RETURNED_CAS_TOKEN: expected_cas_token,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_pool.get_cas_typed(
        key=Key("foo"),
        cls=Foo,
    )
    assert result == expected_value
    assert cas_token == expected_cas_token

    result = cache_pool.get_typed(key=Key("foo"), cls=Foo)
    assert result == expected_value

    result, cas_token = cache_pool.get_cas(key=Key("foo"))
    assert result == expected_value
    assert cas_token == expected_cas_token

    result = cache_pool.get(key=Key("foo"))
    assert result == expected_value


def test_get_other(memcache_socket: MemcacheSocket, cache_pool: FakeCachePool) -> None:
    memcache_socket.get_response.return_value = Success()
    try:
        cache_pool.get(
            key=Key("foo"),
        )
        raise AssertionError("Should not be reached")
    except MemcacheError as e:
        assert "Unexpected response" in str(e)


def test_value_wrong_type(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.RETURNED_CAS_TOKEN: expected_cas_token,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_pool.get_cas_typed(key=Key("foo"), cls=Bar)
    assert result is None
    assert cas_token == expected_cas_token
    memcache_socket.get_value.assert_called_once_with(len(encoded_value.data))

    try:
        cache_pool.get_cas_typed(key=Key("foo"), cls=Bar, error_on_type_mismatch=True)
        raise AssertionError("Should not be reached")
    except ValueError as e:
        assert "Expecting <class 'tests.base.cache_pool_test.Bar'> got Foo" in str(e)

    result = cache_pool.get_typed(key=Key("foo"), cls=Bar)
    assert result is None

    try:
        cache_pool.get_typed(key=Key("foo"), cls=Bar, error_on_type_mismatch=True)
        raise AssertionError("Should not be reached")
    except ValueError as e:
        assert "Expecting <class 'tests.base.cache_pool_test.Bar'> got Foo" in str(e)


@pytest.fixture
def time(mocker: MockerFixture) -> MagicMock:
    return mocker.patch("meta_memcache.base.cache_pool.time", autospec=True)


def test_recache_win_returns_miss(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        flags=set([Flag.WIN, Flag.STALE]),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.RETURNED_CAS_TOKEN: expected_cas_token,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_pool.get_cas(key=Key("foo"))
    assert result is None
    assert cas_token == expected_cas_token


def test_recache_lost_returns_stale_value(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        flags=set([Flag.LOST, Flag.STALE]),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.RETURNED_CAS_TOKEN: expected_cas_token,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_pool.get_cas(key=Key("foo"))
    assert result == expected_value
    assert cas_token == expected_cas_token


def test_get_or_lease_hit(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.RETURNED_CAS_TOKEN: expected_cas_token,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas_token = cache_pool.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result == expected_value
    assert cas_token == expected_cas_token
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v c h f N30\r\n", with_noop=False
    )
    memcache_socket.get_value.assert_called_once_with(len(encoded_value.data))
    time.sleep.assert_not_called()


def test_get_or_lease_miss_win(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas_token = 123
    memcache_socket.get_response.return_value = Value(
        size=0,
        flags=set([Flag.WIN]),
        int_flags={
            IntFlag.RETURNED_CAS_TOKEN: expected_cas_token,
        },
    )
    memcache_socket.get_value.return_value = b""

    result, cas_token = cache_pool.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result is None
    assert cas_token == expected_cas_token
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v c h f N30\r\n", with_noop=False
    )
    memcache_socket.get_value.assert_called_once_with(0)
    time.sleep.assert_not_called()


def test_get_or_lease_miss_lost_then_data(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas_token = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.side_effect = [
        Value(
            size=0,
            flags=set([Flag.LOST]),
            int_flags={
                IntFlag.RETURNED_CAS_TOKEN: expected_cas_token - 1,
            },
        ),
        Value(
            size=0,
            flags=set([Flag.LOST]),
            int_flags={
                IntFlag.RETURNED_CAS_TOKEN: expected_cas_token - 1,
            },
        ),
        Value(
            size=len(encoded_value.data),
            int_flags={
                IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
                IntFlag.RETURNED_CAS_TOKEN: expected_cas_token,
            },
        ),
    ]
    memcache_socket.get_value.side_effect = [b"", b"", encoded_value.data]

    result, cas_token = cache_pool.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result == expected_value
    assert cas_token == expected_cas_token
    memcache_socket.sendall.assert_has_calls(
        [
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
        ]
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
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas_token = 123
    memcache_socket.get_response.side_effect = [
        Value(
            size=0,
            flags=set([Flag.LOST]),
            int_flags={
                IntFlag.RETURNED_CAS_TOKEN: expected_cas_token - 1,
            },
        ),
        Value(
            size=0,
            flags=set([Flag.LOST]),
            int_flags={
                IntFlag.RETURNED_CAS_TOKEN: expected_cas_token - 1,
            },
        ),
        Value(
            size=0,
            flags=set([Flag.WIN]),
            int_flags={
                IntFlag.RETURNED_CAS_TOKEN: expected_cas_token,
            },
        ),
    ]
    memcache_socket.get_value.side_effect = [b"", b"", b""]

    result, cas_token = cache_pool.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result is None
    assert cas_token == expected_cas_token
    memcache_socket.sendall.assert_has_calls(
        [
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
        ]
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
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas_token = 123
    memcache_socket.get_response.return_value = Value(
        size=0,
        flags=set([Flag.LOST]),
        int_flags={
            IntFlag.RETURNED_CAS_TOKEN: expected_cas_token,
        },
    )
    memcache_socket.get_value.return_value = b""

    result, cas_token = cache_pool.get_or_lease_cas(
        key=Key("foo"),
        lease_policy=LeasePolicy(
            miss_retries=4, wait_backoff_factor=10, miss_max_retry_wait=15
        ),
    )
    assert result is None
    assert cas_token == expected_cas_token
    memcache_socket.sendall.assert_has_calls(
        [
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
            call(b"mg foo t l v c h f N30\r\n", with_noop=False),
        ]
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
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool
) -> None:
    # We should never get a miss
    memcache_socket.get_response.return_value = Miss()
    try:
        cache_pool.get_or_lease(
            key=Key("foo"),
            lease_policy=LeasePolicy(),
            touch_ttl=300,
            recache_policy=RecachePolicy(ttl=60),
        )
        raise AssertionError("Should not be reached")
    except MemcacheError:
        pass
    memcache_socket.sendall.assert_called_once_with(
        b"mg foo t l v c h f N30 R60 T300\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()

    try:
        cache_pool.get_or_lease(
            key=Key("foo"),
            lease_policy=LeasePolicy(miss_retries=-10),
            touch_ttl=300,
            recache_policy=RecachePolicy(ttl=60),
        )
        raise AssertionError("Should not be reached")
    except ValueError as e:
        assert "Wrong lease_policy: miss_retries needs to be greater than 0" in str(e)


def test_on_write_failure(
    cache_pool: FakeCachePool,
) -> None:
    failures_tracked: list[Key] = []
    on_failure: Callable[[Key], None] = lambda key: failures_tracked.append(key)
    cache_pool.on_write_failure += on_failure

    cache_pool.connection_pool.get_connection.side_effect = MemcacheServerError(
        server="broken:11211", message="uh-oh"
    )
    try:
        cache_pool.get(key=Key("foo"))
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 0
    failures_tracked.clear()

    try:
        cache_pool.delete(key=Key("foo"))
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 1
    assert failures_tracked[0] == Key("foo")
    failures_tracked.clear()

    try:
        cache_pool.set(key=Key("foo"), value=1, ttl=10)
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    assert len(failures_tracked) == 1
    assert failures_tracked[0] == Key("foo")
    failures_tracked.clear()


def test_write_failure_not_raise_on_server_error(
    cache_pool_not_raise_on_server_error: FakeCachePool,
) -> None:
    cache_pool = cache_pool_not_raise_on_server_error
    failures_tracked: list[Key] = []
    on_failure: Callable[[Key], None] = lambda key: failures_tracked.append(key)
    cache_pool.on_write_failure += on_failure

    cache_pool.connection_pool.get_connection.side_effect = MemcacheServerError(
        server="broken:11211", message="uh-oh"
    )
    result = cache_pool.get(key=Key("foo"))
    assert result is None
    assert len(failures_tracked) == 0
    failures_tracked.clear()

    result = cache_pool.delete(key=Key("foo"))
    assert result is False
    assert len(failures_tracked) == 1
    assert failures_tracked[0] == Key("foo")
    failures_tracked.clear()

    result = cache_pool.set(key=Key("foo"), value=1, ttl=10)
    assert result is False
    assert len(failures_tracked) == 1
    assert failures_tracked[0] == Key("foo")
    failures_tracked.clear()


def test_delta_cmd(memcache_socket: MemcacheSocket, cache_pool: FakeCachePool) -> None:
    memcache_socket.get_response.return_value = Miss()

    cache_pool.delta(key="foo", delta=1, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"ma foo q D1\r\n", with_noop=True)
    memcache_socket.sendall.reset_mock()

    cache_pool.delta(key=Key("foo"), delta=1, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"ma foo q D1\r\n", with_noop=True)
    memcache_socket.sendall.reset_mock()

    cache_pool.delta(key=Key("foo"), delta=1, refresh_ttl=60, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(
        b"ma foo q D1 T60\r\n", with_noop=True
    )
    memcache_socket.sendall.reset_mock()

    cache_pool.delta(key=Key("foo"), delta=-2, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(
        b"ma foo q D2 M-\r\n", with_noop=True
    )
    memcache_socket.sendall.reset_mock()

    cache_pool.delta_initialize(
        key=Key("foo"),
        delta=1,
        initial_value=10,
        initial_ttl=60,
        no_reply=True,
        cas_token=123,
    )
    memcache_socket.sendall.assert_called_once_with(
        b"ma foo q D1 C123 J10 N60\r\n", with_noop=True
    )
    memcache_socket.sendall.reset_mock()

    memcache_socket.get_response.assert_not_called()

    result = cache_pool.delta(key=Key("foo"), delta=1)
    assert result is False
    memcache_socket.sendall.assert_called_once_with(b"ma foo D1\r\n", with_noop=False)
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    result = cache_pool.delta_and_get(key=Key("foo"), delta=1)
    assert result is None
    memcache_socket.sendall.assert_called_once_with(b"ma foo v D1\r\n", with_noop=False)
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    result = cache_pool.delta_initialize_and_get(
        key=Key("foo"), delta=1, initial_value=0, initial_ttl=60
    )
    assert result is None
    memcache_socket.sendall.assert_called_once_with(
        b"ma foo v D1 J0 N60\r\n", with_noop=False
    )
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    memcache_socket.get_response.return_value = Success()

    result = cache_pool.delta(key=Key("foo"), delta=1)
    assert result is True
    memcache_socket.sendall.assert_called_once_with(b"ma foo D1\r\n", with_noop=False)
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    memcache_socket.get_response.return_value = Value(size=2)
    memcache_socket.get_value.return_value = b"10"

    result = cache_pool.delta_and_get(key=Key("foo"), delta=1)
    assert result == 10
    memcache_socket.sendall.assert_called_once_with(b"ma foo v D1\r\n", with_noop=False)
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()
    memcache_socket.get_value.reset_mock()


def test_multi_get(memcache_socket: MemcacheSocket, cache_pool: FakeCachePool) -> None:
    memcache_socket.get_response.side_effect = [
        Miss(),
        Value(size=2, int_flags={IntFlag.CLIENT_FLAG: MixedSerializer.BINARY}),
        Value(size=2, flags=set([Flag.WIN])),
    ]
    memcache_socket.get_value.return_value = b"OK"

    results = cache_pool.multi_get(
        keys=[
            Key("miss"),
            Key("found"),
            Key("lease"),
        ]
    )
    assert memcache_socket.sendall.mock_calls == [
        call(b"mg miss t l v h f\r\n", with_noop=False),
        call(b"mg found t l v h f\r\n", with_noop=False),
        call(b"mg lease t l v h f\r\n", with_noop=False),
    ]
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
