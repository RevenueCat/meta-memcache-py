import socket
from typing import Callable, List

import pytest
from pytest_mock import MockerFixture

from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.errors import MemcacheError
from meta_memcache.protocol import (
    Conflict,
    Flag,
    IntFlag,
    Miss,
    NotStored,
    ServerVersion,
    Success,
    TokenFlag,
    Value,
)


def recv_into_mock(datas: List[bytes]) -> Callable[[memoryview], int]:
    def recv_into(buffer: memoryview, length: int = 0, flags: int = 0) -> int:
        if not datas:
            return -1
        data = datas[0]
        data_size = len(data)
        if length > 0:
            buffer_size = length
        else:
            buffer_size = len(buffer)
        if data_size > buffer_size:
            read = buffer_size
            buffer[:] = data[0:buffer_size]
            datas[0] = data[buffer_size:]
        else:
            read = data_size
            buffer[0:data_size] = data
            datas.pop(0)
        return read

    return recv_into


@pytest.fixture
def fake_socket(mocker: MockerFixture) -> socket.socket:
    return mocker.MagicMock(spec=socket.socket)


def test_get_response(
    fake_socket: socket.socket,
) -> None:
    fake_socket.recv_into.side_effect = recv_into_mock(
        [b"EN\r\n", b"NF\r\nNS", b"\r\nE", b"X\r\nXX\r\n"]
    )
    ms = MemcacheSocket(fake_socket)
    assert isinstance(ms.get_response(), Miss)
    assert isinstance(ms.get_response(), Miss)
    assert isinstance(ms.get_response(), NotStored)
    assert isinstance(ms.get_response(), Conflict)
    try:
        ms.get_response()
        raise AssertionError("Should not be reached")
    except MemcacheError as e:
        assert "Error parsing response header" in str(e)

    fake_socket.recv_into.side_effect = recv_into_mock(
        [b"HD c1\r\nVA 2 c1", b"\r\nOK\r\n"]
    )
    ms = MemcacheSocket(fake_socket)
    result = ms.get_response()
    assert isinstance(result, Success)
    assert result.int_flags == {IntFlag.RETURNED_CAS_TOKEN: 1}

    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.int_flags == {IntFlag.RETURNED_CAS_TOKEN: 1}
    assert result.size == 2


def test_get_response_1_6_6(
    fake_socket: socket.socket,
) -> None:
    fake_socket.recv_into.side_effect = recv_into_mock(
        [b"OK c1\r\nVA 2 c1", b"\r\nOK\r\n"]
    )
    ms = MemcacheSocket(fake_socket, version=ServerVersion.AWS_1_6_6)
    result = ms.get_response()
    assert isinstance(result, Success)
    assert result.int_flags == {IntFlag.RETURNED_CAS_TOKEN: 1}

    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.int_flags == {IntFlag.RETURNED_CAS_TOKEN: 1}
    assert result.size == 2


def test_noreply(
    fake_socket: socket.socket,
) -> None:
    fake_socket.recv_into.side_effect = recv_into_mock(
        [b"EX\r\n", b"MN", b"\r\nHD", b"\r\n"]
    )
    ms = MemcacheSocket(fake_socket)
    ms.sendall(b"test", with_noop=True)
    # The first EX should be skipped as it is before the No-op
    # response, so this should be a success:
    assert isinstance(ms.get_response(), Success)


def test_get_value(
    fake_socket: socket.socket,
) -> None:
    fake_socket.recv_into.side_effect = recv_into_mock([b"VA 2 c1\r\nOK\r\n"])
    ms = MemcacheSocket(fake_socket)
    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.int_flags == {IntFlag.RETURNED_CAS_TOKEN: 1}
    assert result.size == 2
    ms.get_value(2)


def test_get_value_large(
    fake_socket: socket.socket,
) -> None:
    fake_socket.recv_into.side_effect = recv_into_mock(
        [b"VA 200 c1  Oxxx W Q Qa  \r\n", b"1234567890", b"1234567890" * 19 + b"\r\n"],
    )
    ms = MemcacheSocket(fake_socket, buffer_size=100)
    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.int_flags == {IntFlag.RETURNED_CAS_TOKEN: 1}
    assert result.flags == set([Flag.WIN])
    assert result.token_flags == {TokenFlag.OPAQUE: b"xxx"}
    assert result.size == 200
    value = ms.get_value(result.size)
    assert len(value) == result.size
    assert value == b"1234567890" * 20


def test_get_value_with_incomplete_endl(
    fake_socket: socket.socket,
) -> None:
    data = b"VA 10\r\n1234567890\r\n"
    fake_socket.recv_into.side_effect = recv_into_mock([data])
    ms = MemcacheSocket(fake_socket, buffer_size=len(data) - 1)
    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.size == 10
    value = ms.get_value(result.size)
    assert len(value) == result.size
    assert value == b"1234567890"

    fake_socket.recv_into.side_effect = recv_into_mock([data])
    ms = MemcacheSocket(fake_socket, buffer_size=len(data) - 2)
    result = ms.get_response()
    assert isinstance(result, Value)
    assert result.size == 10
    value = ms.get_value(result.size)
    assert len(value) == result.size
    assert value == b"1234567890"


def test_bad(
    fake_socket: socket.socket,
) -> None:
    fake_socket.recv_into.side_effect = recv_into_mock(
        [b"VA 10 c1\r\n", b"1234567890XX"]
    )
    ms = MemcacheSocket(fake_socket, buffer_size=100)
    result = ms.get_response()
    try:
        ms.get_value(result.size)
        raise AssertionError("Should not be reached")
    except MemcacheError as e:
        assert "Error parsing value" in str(e)

    fake_socket.recv_into.side_effect = recv_into_mock(
        [b"VA 200 c1\r\n", b"1234567890", b"1234567890" * 19 + b"XX"],
    )
    ms = MemcacheSocket(fake_socket, buffer_size=100)
    result = ms.get_response()
    try:
        ms.get_value(result.size)
        raise AssertionError("Should not be reached")
    except MemcacheError as e:
        assert "Error parsing value" in str(e)

    fake_socket.recv_into.side_effect = recv_into_mock([b"VA 10 c1", b"XX"])
    ms = MemcacheSocket(fake_socket, buffer_size=100)
    try:
        ms.get_response()
        raise AssertionError("Should not be reached")
    except MemcacheError as e:
        assert "Bad response" in str(e)


def test_reset_buffer(
    fake_socket: socket.socket,
) -> None:
    data = b"VA 50 \r\n" + (b"1234567890" * 5) + b"\r\n"
    fake_socket.recv_into.side_effect = recv_into_mock([data])
    ms = MemcacheSocket(fake_socket, buffer_size=len(data) - 1)
    result = ms.get_response()
    value = ms.get_value(result.size)
    assert len(value) == result.size
    assert value == b"1234567890" * 5
    ms._reset_buffer()
    assert ms._pos == 0

    data = (b"VA 50 \r\n" + (b"1234567890" * 5) + b"\r\n") * 2
    fake_socket.recv_into.side_effect = recv_into_mock([data])
    ms = MemcacheSocket(fake_socket, buffer_size=len(data) - 10)
    result = ms.get_response()
    value = ms.get_value(result.size)
    assert len(value) == result.size
    assert value == b"1234567890" * 5
    ms._reset_buffer()
    assert ms._pos == len(data) // 2
    result = ms.get_response()
    value = ms.get_value(result.size)
    assert len(value) == result.size
    assert value == b"1234567890" * 5
    ms._reset_buffer()
    assert ms._pos == 0


def test_close(
    fake_socket: socket.socket,
) -> None:
    ms = MemcacheSocket(fake_socket, buffer_size=100)
    ms.close()
    fake_socket.close.assert_called_once()
