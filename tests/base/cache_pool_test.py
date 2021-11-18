import pickle
import zlib
from dataclasses import dataclass
from unittest.mock import MagicMock, call

import pytest
from pytest_mock import MockerFixture

from meta_memcache import Key, MemcacheError
from meta_memcache.base.base_write_failure_tracker import BaseWriteFailureTracker
from meta_memcache.base.cache_pool import CachePool
from meta_memcache.base.connection_pool import ConnectionPool
from meta_memcache.base.memcache_socket import MemcacheSocket
from meta_memcache.configuration import (
    LeasePolicy,
    RecachePolicy,
    StalePolicy,
    default_binary_key_encoding,
)
from meta_memcache.errors import MemcacheServerError
from meta_memcache.protocol import Flag, IntFlag, Miss, NotStored, Success, Value
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
def write_failure_tracker(mocker: MockerFixture) -> BaseWriteFailureTracker:
    return mocker.MagicMock(spec=BaseWriteFailureTracker)


@pytest.fixture
def memcache_socket(mocker: MockerFixture) -> MemcacheSocket:
    return mocker.MagicMock(sped=MemcacheSocket)


@pytest.fixture
def cache_pool(
    mocker: MockerFixture,
    memcache_socket: MemcacheSocket,
    write_failure_tracker: BaseWriteFailureTracker,
) -> FakeCachePool:
    connection_pool = mocker.MagicMock(sped=ConnectionPool)
    connection_pool.get_connection().__enter__.return_value = memcache_socket
    return FakeCachePool(
        connection_pool=connection_pool,
        serializer=MixedSerializer(),
        binary_key_encoding_fn=default_binary_key_encoding,
        write_failure_tracker=write_failure_tracker,
    )


def test_set_cmd(
    memcache_socket: MemcacheSocket,
    cache_pool: FakeCachePool,
) -> None:
    memcache_socket.get_response.return_value = Success()

    cache_pool.set(key=Key("foo"), value="bar", ttl=300)
    memcache_socket.sendall.assert_called_once_with(b"ms foo 3 F0 T300\r\nbar\r\n")
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=123, ttl=300)
    memcache_socket.sendall.assert_called_once_with(b"ms foo 3 F2 T300\r\n123\r\n")
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    value = [1, 2, 3]
    data = pickle.dumps(value, protocol=0)
    cache_pool.set(key=Key("foo"), value=value, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo " + str(len(data)).encode() + b" F1 T300\r\n" + data + b"\r\n"
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=b"123", ttl=300)
    memcache_socket.sendall.assert_called_once_with(b"ms foo 3 F16 T300\r\n123\r\n")
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    value = b"123" * 100
    data = zlib.compress(value)
    cache_pool.set(key=Key("foo"), value=value, ttl=300)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo " + str(len(data)).encode() + b" F24 T300\r\n" + data + b"\r\n"
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=b"123", ttl=300, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"ms foo 3 F16 T300 q\r\n123\r\n")
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(key=Key("foo"), value=b"123", ttl=300, cas=666)
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 F16 T300 c666\r\n123\r\n"
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(
        key=Key("foo"), value=b"123", ttl=300, cas=666, stale_policy=StalePolicy()
    )
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 F16 T300 c666\r\n123\r\n"
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.set(
        key=Key("foo"),
        value=b"123",
        ttl=300,
        cas=666,
        stale_policy=StalePolicy(mark_stale_on_cas_mismatch=True),
    )
    memcache_socket.sendall.assert_called_once_with(
        b"ms foo 3 F16 I T300 c666\r\n123\r\n"
    )
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()


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

    cache_pool.delete(key=Key("foo"))
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n")
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.delete(key=Key("foo"), cas=666)
    memcache_socket.sendall.assert_called_once_with(b"md foo c666\r\n")
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.delete(key=Key("foo"), no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"md foo q\r\n")
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.delete(key=Key("foo"), stale_policy=StalePolicy())
    memcache_socket.sendall.assert_called_once_with(b"md foo\r\n")
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.delete(
        key=Key("foo"),
        stale_policy=StalePolicy(mark_stale_on_deletion_ttl=30),
    )
    memcache_socket.sendall.assert_called_once_with(b"md foo I T30\r\n")
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

    cache_pool.touch(key=Key("foo"), ttl=60)
    memcache_socket.sendall.assert_called_once_with(b"mg foo T60\r\n")
    memcache_socket.get_response.assert_called_once_with()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()

    cache_pool.touch(key=Key("foo"), ttl=60, no_reply=True)
    memcache_socket.sendall.assert_called_once_with(b"mg foo T60 q\r\n")
    memcache_socket.get_response.assert_not_called()
    memcache_socket.sendall.reset_mock()
    memcache_socket.get_response.reset_mock()


def test_get_cmd(memcache_socket: MemcacheSocket, cache_pool: FakeCachePool) -> None:
    memcache_socket.get_response.return_value = Miss()

    cache_pool.get(key=Key("foo"))
    memcache_socket.sendall.assert_called_once_with(b"mg foo c f h l t v\r\n")
    memcache_socket.sendall.reset_mock()

    cache_pool.get(key=Key("foo"), touch_ttl=300)
    memcache_socket.sendall.assert_called_once_with(b"mg foo T300 c f h l t v\r\n")
    memcache_socket.sendall.reset_mock()

    cache_pool.get(key=Key("foo"), recache_policy=RecachePolicy())
    memcache_socket.sendall.assert_called_once_with(b"mg foo R30 c f h l t v\r\n")
    memcache_socket.sendall.reset_mock()

    cache_pool.get(key=Key("foo"), touch_ttl=300, recache_policy=RecachePolicy())
    memcache_socket.sendall.assert_called_once_with(b"mg foo R30 T300 c f h l t v\r\n")
    memcache_socket.sendall.reset_mock()

    cache_pool.get(
        key=Key("large_key" * 50), touch_ttl=300, recache_policy=RecachePolicy()
    )
    memcache_socket.sendall.assert_called_once_with(
        b"mg 4gCNJuSyOJPGW8kRddioRlPx R30 T300 b c f h l t v\r\n"
    )
    memcache_socket.sendall.reset_mock()

    cache_pool.get(
        key=Key("úníçod⍷", is_unicode=True),
        touch_ttl=300,
        recache_policy=RecachePolicy(),
    )
    memcache_socket.sendall.assert_called_once_with(
        b"mg lCV3WxKxtWrdY4s1+R710+9J R30 T300 b c f h l t v\r\n"
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
        b"mg foo N30 R60 T300 c f h l t v\r\n"
    )
    memcache_socket.sendall.reset_mock()


def test_get_miss(memcache_socket: MemcacheSocket, cache_pool: FakeCachePool) -> None:
    memcache_socket.get_response.return_value = Miss()

    result, cas = cache_pool.get_cas_typed(key=Key("foo"), cls=Foo)
    assert result is None
    assert cas is None
    memcache_socket.sendall.assert_called_once_with(b"mg foo c f h l t v\r\n")
    memcache_socket.sendall.reset_mock()

    result = cache_pool.get_typed(key=Key("foo"), cls=Foo)
    assert result is None
    memcache_socket.sendall.assert_called_once_with(b"mg foo c f h l t v\r\n")
    memcache_socket.sendall.reset_mock()

    result, cas = cache_pool.get_cas(key=Key("foo"))
    assert result is None
    assert cas is None
    memcache_socket.sendall.assert_called_once_with(b"mg foo c f h l t v\r\n")
    memcache_socket.sendall.reset_mock()

    result = cache_pool.get(key=Key("foo"))
    assert result is None
    memcache_socket.sendall.assert_called_once_with(b"mg foo c f h l t v\r\n")
    memcache_socket.sendall.reset_mock()


def test_get_value(memcache_socket: MemcacheSocket, cache_pool: FakeCachePool) -> None:
    expected_cas = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.CAS: expected_cas,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas = cache_pool.get_cas_typed(
        key=Key("foo"),
        cls=Foo,
    )
    assert result == expected_value
    assert cas == expected_cas

    result = cache_pool.get_typed(key=Key("foo"), cls=Foo)
    assert result == expected_value

    result, cas = cache_pool.get_cas(key=Key("foo"))
    assert result == expected_value
    assert cas == expected_cas

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
    expected_cas = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.CAS: expected_cas,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas = cache_pool.get_cas_typed(key=Key("foo"), cls=Bar)
    assert result is None
    assert cas == expected_cas
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
    expected_cas = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        flags=set([Flag.WIN, Flag.STALE]),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.CAS: expected_cas,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas = cache_pool.get_cas(key=Key("foo"))
    assert result is None
    assert cas == expected_cas


def test_recache_lost_returns_stale_value(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        flags=set([Flag.LOST, Flag.STALE]),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.CAS: expected_cas,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas = cache_pool.get_cas(key=Key("foo"))
    assert result == expected_value
    assert cas == expected_cas


def test_get_or_lease_hit(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.return_value = Value(
        size=len(encoded_value.data),
        int_flags={
            IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
            IntFlag.CAS: expected_cas,
        },
    )
    memcache_socket.get_value.return_value = encoded_value.data

    result, cas = cache_pool.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result == expected_value
    assert cas == expected_cas
    memcache_socket.sendall.assert_called_once_with(b"mg foo N30 c f h l t v\r\n")
    memcache_socket.get_value.assert_called_once_with(len(encoded_value.data))
    time.sleep.assert_not_called()


def test_get_or_lease_miss_win(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas = 123
    memcache_socket.get_response.return_value = Value(
        size=0,
        flags=set([Flag.WIN]),
        int_flags={
            IntFlag.CAS: expected_cas,
        },
    )
    memcache_socket.get_value.return_value = b""

    result, cas = cache_pool.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result is None
    assert cas == expected_cas
    memcache_socket.sendall.assert_called_once_with(b"mg foo N30 c f h l t v\r\n")
    memcache_socket.get_value.assert_called_once_with(0)
    time.sleep.assert_not_called()


def test_get_or_lease_miss_lost_then_data(
    memcache_socket: MemcacheSocket, cache_pool: FakeCachePool, time: MagicMock
) -> None:
    expected_cas = 123
    expected_value = Foo("hello world")
    encoded_value = MixedSerializer().serialize(expected_value)
    memcache_socket.get_response.side_effect = [
        Value(
            size=0,
            flags=set([Flag.LOST]),
            int_flags={
                IntFlag.CAS: expected_cas - 1,
            },
        ),
        Value(
            size=0,
            flags=set([Flag.LOST]),
            int_flags={
                IntFlag.CAS: expected_cas - 1,
            },
        ),
        Value(
            size=len(encoded_value.data),
            int_flags={
                IntFlag.CLIENT_FLAG: encoded_value.encoding_id,
                IntFlag.CAS: expected_cas,
            },
        ),
    ]
    memcache_socket.get_value.side_effect = [b"", b"", encoded_value.data]

    result, cas = cache_pool.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result == expected_value
    assert cas == expected_cas
    memcache_socket.sendall.assert_has_calls(
        [
            call(b"mg foo N30 c f h l t v\r\n"),
            call(b"mg foo N30 c f h l t v\r\n"),
            call(b"mg foo N30 c f h l t v\r\n"),
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
    expected_cas = 123
    memcache_socket.get_response.side_effect = [
        Value(
            size=0,
            flags=set([Flag.LOST]),
            int_flags={
                IntFlag.CAS: expected_cas - 1,
            },
        ),
        Value(
            size=0,
            flags=set([Flag.LOST]),
            int_flags={
                IntFlag.CAS: expected_cas - 1,
            },
        ),
        Value(
            size=0,
            flags=set([Flag.WIN]),
            int_flags={
                IntFlag.CAS: expected_cas,
            },
        ),
    ]
    memcache_socket.get_value.side_effect = [b"", b"", b""]

    result, cas = cache_pool.get_or_lease_cas(
        key=Key("foo"), lease_policy=LeasePolicy()
    )
    assert result is None
    assert cas == expected_cas
    memcache_socket.sendall.assert_has_calls(
        [
            call(b"mg foo N30 c f h l t v\r\n"),
            call(b"mg foo N30 c f h l t v\r\n"),
            call(b"mg foo N30 c f h l t v\r\n"),
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
    expected_cas = 123
    memcache_socket.get_response.return_value = Value(
        size=0,
        flags=set([Flag.LOST]),
        int_flags={
            IntFlag.CAS: expected_cas,
        },
    )
    memcache_socket.get_value.return_value = b""

    result, cas = cache_pool.get_or_lease_cas(
        key=Key("foo"),
        lease_policy=LeasePolicy(
            miss_retries=4, wait_backoff_factor=10, miss_max_retry_wait=15
        ),
    )
    assert result is None
    assert cas == expected_cas
    memcache_socket.sendall.assert_has_calls(
        [
            call(b"mg foo N30 c f h l t v\r\n"),
            call(b"mg foo N30 c f h l t v\r\n"),
            call(b"mg foo N30 c f h l t v\r\n"),
            call(b"mg foo N30 c f h l t v\r\n"),
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
        b"mg foo N30 R60 T300 c f h l t v\r\n"
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


def test_write_failure_tracker(
    # memcache_socket: MemcacheSocket,
    cache_pool: FakeCachePool,
    write_failure_tracker: BaseWriteFailureTracker,
) -> None:
    cache_pool.connection_pool.get_connection.side_effect = MemcacheServerError(
        server="broken:11211", message="uh-oh"
    )
    try:
        cache_pool.get(key=Key("foo"))
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    write_failure_tracker.add_key.assert_not_called()

    try:
        cache_pool.delete(key=Key("foo"))
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    write_failure_tracker.add_key.assert_called_once_with(Key("foo"))
    write_failure_tracker.add_key.reset_mock()

    try:
        cache_pool.set(key=Key("foo"), value=1, ttl=10)
        raise AssertionError("Should not be reached")
    except MemcacheServerError as e:
        assert "uh-oh" in str(e)
    write_failure_tracker.add_key.assert_called_once_with(Key("foo"))
    write_failure_tracker.add_key.reset_mock()
