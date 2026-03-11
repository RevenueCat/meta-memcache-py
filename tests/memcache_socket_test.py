import socket

import pytest

from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.protocol import (
    Conflict,
    Miss,
    NotStored,
    ServerVersion,
    Success,
    Value,
)


@pytest.fixture
def socket_pair():
    a, b = socket.socketpair()
    yield a, b
    a.close()
    b.close()


def test_get_response(socket_pair: tuple[socket.socket, socket.socket]) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a)

    b.sendall(b"EN\r\nNF\r\nNS\r\nEX\r\n")
    assert isinstance(ms.get_response(), Miss)
    assert isinstance(ms.get_response(), Miss)
    assert isinstance(ms.get_response(), NotStored)
    assert isinstance(ms.get_response(), Conflict)

    # Close the write end to trigger error on next read
    b.close()
    with pytest.raises(ConnectionError):
        ms.get_response()


def test_get_response_success_and_value(
    socket_pair: tuple[socket.socket, socket.socket],
) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a)

    b.sendall(b"HD c1\r\nVA 2 c1\r\nOK\r\n")
    result = ms.get_response()
    assert isinstance(result, Success)
    assert result.flags.cas_token == 1

    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.flags.cas_token == 1
    assert result.size == 2
    assert result.value == b"OK"


def test_get_response_1_6_6(
    socket_pair: tuple[socket.socket, socket.socket],
) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a, version=ServerVersion.AWS_1_6_6)

    b.sendall(b"OK c1\r\nVA 2 c1\r\nOK\r\n")
    result = ms.get_response()
    assert isinstance(result, Success)
    assert result.flags.cas_token == 1

    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.flags.cas_token == 1
    assert result.size == 2
    assert result.value == b"OK"


def test_noreply(socket_pair: tuple[socket.socket, socket.socket]) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a)

    ms.sendall(b"test", with_noop=True)
    # The first EX should be skipped as it is before the No-op
    # response, so this should be a success:
    b.sendall(b"EX\r\nMN\r\nHD\r\n")
    assert isinstance(ms.get_response(), Success)


def test_socket_closed(socket_pair: tuple[socket.socket, socket.socket]) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a)
    b.close()
    with pytest.raises(ConnectionError):
        ms.get_response()


def test_get_value(socket_pair: tuple[socket.socket, socket.socket]) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a)

    b.sendall(b"VA 2 c1\r\nOK\r\n")
    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.flags.cas_token == 1
    assert result.size == 2
    assert result.value == b"OK"


def test_get_value_large(socket_pair: tuple[socket.socket, socket.socket]) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a, buffer_size=100)

    b.sendall(b"VA 200 c1  Oxxx W Q Qa  \r\n" + b"1234567890" * 20 + b"\r\n")
    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.flags.cas_token == 1
    assert result.flags.win is True
    assert bytes(result.flags.opaque) == b"xxx"
    assert result.size == 200
    assert len(result.value) == result.size
    assert result.value == b"1234567890" * 20


def test_get_value_with_incomplete_endl(
    socket_pair: tuple[socket.socket, socket.socket],
) -> None:
    a, b = socket_pair
    # Use a small buffer so the endl may be split
    ms = MemcacheSocket(a, buffer_size=18)

    b.sendall(b"VA 10\r\n1234567890\r\n")
    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.size == 10
    assert len(result.value) == result.size
    assert result.value == b"1234567890"


def test_unknown_response(socket_pair: tuple[socket.socket, socket.socket]) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a)

    b.sendall(b"XX\r\n")
    with pytest.raises(ConnectionError):
        ms.get_response()


def test_bad_value_termination(
    socket_pair: tuple[socket.socket, socket.socket],
) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a, buffer_size=100)

    # Value terminated with XX instead of \r\n
    b.sendall(b"VA 10 c1\r\n1234567890XX")
    with pytest.raises(ConnectionError):
        ms.get_response()


def test_bad_large_value_termination(
    socket_pair: tuple[socket.socket, socket.socket],
) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a, buffer_size=100)

    # Large value (exceeds buffer) terminated with XX instead of \r\n
    b.sendall(b"VA 200 c1\r\n" + b"1234567890" * 20 + b"XX")
    with pytest.raises(ConnectionError):
        ms.get_response()


def test_sequential_reads_small_buffer(
    socket_pair: tuple[socket.socket, socket.socket],
) -> None:
    """Test multiple response+value sequences with a small buffer.

    This exercises the internal buffer reset logic that occurs when
    multiple responses are read in sequence and the buffer fills up.
    """
    a, b = socket_pair
    ms = MemcacheSocket(a, buffer_size=60)

    data = b"VA 50 \r\n" + (b"1234567890" * 5) + b"\r\n"
    b.sendall(data * 2)

    result = ms.get_response()
    assert len(result.value) == result.size
    assert result.value == b"1234567890" * 5

    result = ms.get_response()
    assert len(result.value) == result.size
    assert result.value == b"1234567890" * 5


def test_close(socket_pair: tuple[socket.socket, socket.socket]) -> None:
    a, b = socket_pair
    ms = MemcacheSocket(a, buffer_size=100)
    ms.close()
    # Socket should be closed
    assert a.fileno() == -1
