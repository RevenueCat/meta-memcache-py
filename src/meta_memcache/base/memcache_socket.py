import logging
import socket
from typing import Iterable, List, Union

from meta_memcache.errors import MemcacheError
from meta_memcache.protocol import (
    ENDL,
    ENDL_LEN,
    NOOP,
    SPACE,
    Conflict,
    Miss,
    NotStored,
    ServerVersion,
    Success,
    Value,
    flag_values,
    get_store_success_response_header,
    int_flags_values,
    token_flags_values,
)

_log: logging.Logger = logging.getLogger(__name__)


class MemcacheSocket:
    """
    Wraps a socket and offers the parsing logic for reading headers
    and values from memcache responses.
    parsing logic.

    Uses an internal bytearray as buffer. Try to provide a size that
    fits majority of the responses and it is a power of 2 for optimal
    performance.

    The buffer is thread and greenlet safe because it is bound to
    the socket, and sockets are not shared, just borrowed from
    the connection pool.

    This tries to reduce memory allocation and memory copies to
    a mimimum:
    - We read into the pre-allocated buffer instead of letting
      socket.recv() allocate new byte arrays.
    - We return a memory view to the internal buffer when possible,
      instead of slicing bytes.
    - If data > buffer, we allocate a buffer for the whole data
      to be returned. In this case, we do pay a single extra
      allocation.
    - The returned memoryview's can be used to unserialize so, for
      small memcache values, the only allocation is that of serializer
      building the objects from the bytes in the response.
    - The buffer can hold several responses, it is only reset
      when the capacity is less than half, causing data copy of
      remaining bytes. If there is no remaining bytes (and this is
      the most likely scenario), the buffer is reset with no cost.
    """

    def __init__(
        self,
        conn: socket.socket,
        buffer_size: int = 4096,
        version: ServerVersion = ServerVersion.STABLE,
    ) -> None:
        self.set_socket(conn)
        self._buffer_size = buffer_size
        self._recv_buffer_size = buffer_size
        self._reset_buffer_size: int = buffer_size * 3 // 4
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self._recv_buffer_size)
        self._version = version
        self._store_success_response_header: bytes = get_store_success_response_header(
            version
        )
        self._buf = bytearray(self._buffer_size)
        self._buf_view = memoryview(self._buf)
        self._noop_expected = 0
        self._endl_buf = bytearray(ENDL_LEN)

    def __str__(self) -> str:
        return f"<MemcacheSocket {self._conn.fileno()}>"

    def get_version(self) -> ServerVersion:
        return self._version

    def set_socket(self, conn: socket.socket) -> None:
        """
        This replaces the internal socket and resets the buffer
        """
        self._conn = conn
        self._pos = 0
        self._read = 0

    def close(self) -> None:
        self._conn.close()
        self._pos = 0
        self._read = 0

    def _recv_info_buffer(self) -> int:
        read = self._conn.recv_into(self._buf_view[self._read :])
        if read > 0:
            self._read += read
        return read

    def _recv_fill_buffer(self, buffer: memoryview) -> int:
        read = 0
        size = len(buffer)
        while read < size:
            chunk_size = self._conn.recv_into(
                buffer[read:], size - read, socket.MSG_WAITALL
            )
            if chunk_size > 0:
                read += chunk_size
        return read

    def _reset_buffer(self) -> None:
        """
        Reset buffer moving remaining bytes in it (if any)
        """
        remaining_data = self._read - self._pos
        if remaining_data > 0:
            if self._pos <= self._reset_buffer_size:
                # Avoid moving memory if buffer still has
                # spare capacity for new responses. If the
                # whole buffer us used, we will just reset
                # the pointers and save a lot of memory
                # data copies
                return
            self._buf_view[0:remaining_data] = self._buf_view[self._pos : self._read]
        self._pos = 0
        self._read = remaining_data

    def _recv_header(self) -> memoryview:
        endl_pos = self._buf.find(ENDL, self._pos, self._read)
        while endl_pos < 0 and self._read < self._buffer_size:
            # Missing data, but still space in buffer, so read more
            if self._recv_info_buffer() < 0:
                break
            endl_pos = self._buf.find(ENDL, self._pos, self._read)

        if endl_pos < 0:
            raise MemcacheError("Bad response. Socket might have closed unexpectedly")

        header = self._buf_view[self._pos : endl_pos]
        self._pos = endl_pos + ENDL_LEN
        return header

    def _add_flags(self, success: Success, chunks: Iterable[memoryview]) -> None:
        """
        Each flag starts with one byte for the flag, and and optional int/byte
        value depending on the flag.
        """
        for chunk in chunks:
            flag = chunk[0]
            if len(chunk) == 1:
                # Flag without value
                if f := flag_values.get(flag):
                    success.flags.add(f)
                else:
                    _log.warning(f"Unrecognized flag {bytes(chunk)!r}")
            else:
                # Value flag
                if int_flag := int_flags_values.get(flag):
                    success.int_flags[int_flag] = int(chunk[1:])
                elif token_flag := token_flags_values.get(flag):
                    success.token_flags[token_flag] = bytes(chunk[1:])
                else:
                    _log.warning(f"Unrecognized flag {bytes(chunk)!r}")

    def _tokenize_header(self, header: memoryview) -> List[memoryview]:
        """
        Slice header by spaces into memoryview chunks
        """
        chunks = []
        prev, i = 0, -1
        for i, v in enumerate(header):
            if v == SPACE:
                if i > prev:
                    chunks.append(header[prev:i])
                prev = i + 1
        if prev <= i:
            chunks.append(header[prev:])
        return chunks

    def _get_single_header(self) -> List[memoryview]:
        self._reset_buffer()
        return self._tokenize_header(self._recv_header())

    def sendall(self, data: bytes, with_noop: bool = False) -> None:
        if with_noop:
            self._noop_expected += 1
            data += NOOP
        self._conn.sendall(data)

    def _read_until_noop_header(self) -> None:
        while self._noop_expected > 0:
            response_code, *_chunks = self._get_single_header()
            if response_code == b"MN":
                self._noop_expected -= 1

    def _get_header(self) -> List[memoryview]:
        try:
            if self._noop_expected > 0:
                self._read_until_noop_header()

            return self._get_single_header()
        except Exception as e:
            _log.exception(f"Error reading header from socket in {self}")
            raise MemcacheError(f"Error reading header from socket: {e}") from e

    def get_response(
        self,
    ) -> Union[Value, Success, NotStored, Conflict, Miss]:
        header = self._get_header()
        result: Union[Value, Success, NotStored, Conflict, Miss]
        try:
            response_code, *chunks = header
            if response_code == b"VA":
                # Value response, parse size and flags
                value_size = int(chunks.pop(0))
                result = Value(value_size)
                self._add_flags(result, chunks)
            elif response_code == self._store_success_response_header:
                # Stored or no value, return Success
                result = Success()
                self._add_flags(result, chunks)
            elif response_code == b"NS":
                # Value response, parse size and flags
                result = NotStored()
                assert len(chunks) == 0  # noqa: S101
            elif response_code == b"EX":
                # Already exists, not changed, CAS conflict
                result = Conflict()
                assert len(chunks) == 0  # noqa: S101
            elif response_code == b"EN" or response_code == b"NF":
                # Not Found, Miss.
                result = Miss()
                assert len(chunks) == 0  # noqa: S101
            else:
                raise MemcacheError(f"Unknown response: {bytes(response_code)!r}")
        except Exception as e:
            response = b" ".join(header).decode()
            _log.exception(f"Error parsing response header in {self}: {response}")
            raise MemcacheError(f"Error parsing response header {response}") from e

        return result

    def get_value(self, size: int) -> memoryview:
        """
        Get data value from the buffer and/or socket if hasn't been
        read fully.
        """
        message_size = size + ENDL_LEN
        data_in_buf = self._read - self._pos
        while data_in_buf < message_size and self._read < self._buffer_size:
            # Missing data, but still space in buffer, so read more
            self._recv_info_buffer()
            data_in_buf = self._read - self._pos

        if data_in_buf >= size:
            # Value is within buffer, slice it
            data_end = self._pos + size
            data = self._buf_view[self._pos : data_end]
            if data_in_buf >= size + ENDL_LEN:
                # ENDL is also within buffer, slice it
                endl = self._buf_view[data_end : data_end + ENDL_LEN]
            else:
                # ENDL ended half-way at the end of the buffer.
                endl = memoryview(self._endl_buf)
                endl_in_buf = data_in_buf - size
                # Copy data in the local buffer
                endl[0:endl_in_buf] = self._buf_view[data_end:]
                # Read the rest
                self._recv_fill_buffer(endl[endl_in_buf:])
        else:
            message = bytearray(size + ENDL_LEN)
            message_view = memoryview(message)
            # Copy data in the local buffer to the new allocated buffer
            message_view[:data_in_buf] = self._buf_view[self._pos : self._read]
            # Read the rest
            self._recv_fill_buffer(message_view[data_in_buf:])
            data = message_view[:size]
            endl = message_view[size:]

        self._pos = min(self._pos + message_size, self._read)

        if len(data) != size or endl != ENDL:
            raise MemcacheError(
                f"Error parsing value: Expected {size} bytes, "
                f"terminated in \\r\\n, got {len(data)} bytes: "
                f"{bytes(data)+bytes(endl)!r}",
            )
        return data
