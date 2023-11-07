import logging
import socket
from typing import Optional, Tuple, Union

import meta_memcache_socket

from meta_memcache.errors import MemcacheError
from meta_memcache.protocol import (
    ENDL,
    ENDL_LEN,
    NOOP,
    Conflict,
    Miss,
    NotStored,
    ServerVersion,
    Success,
    ResponseFlags,
    Value,
    get_store_success_response_header,
)

_log: logging.Logger = logging.getLogger(__name__)
NOT_STORED = NotStored()
MISS = Miss()
CONFLICT = Conflict()


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
        self._buf_view[0:remaining_data] = self._buf_view[self._pos : self._read]
        self._pos = 0
        self._read = remaining_data

    def _get_single_header(
        self,
    ) -> Tuple[int, Optional[int], Optional[int], Optional[ResponseFlags]]:
        # Reset buffer for new data
        if self._read == self._pos:
            self._read = 0
            self._pos = 0
        elif self._pos > self._reset_buffer_size:
            self._reset_buffer()

        while True:
            if self._read != self._pos:
                # We have data in the buffer: Try to find the header
                if header_data := meta_memcache_socket.parse_header(
                    self._buf_view, self._pos, self._read
                ):
                    self._pos = header_data[0]
                    return header_data
            if self._recv_info_buffer() <= 0:
                break

        raise MemcacheError("Bad response. Socket might have closed unexpectedly")

    def sendall(self, data: bytes, with_noop: bool = False) -> None:
        if with_noop:
            self._noop_expected += 1
            data += NOOP
        self._conn.sendall(data)

    def _read_until_noop_header(self) -> None:
        while self._noop_expected > 0:
            header = self._get_single_header()
            if header[1] == meta_memcache_socket.RESPONSE_NOOP:
                self._noop_expected -= 1

    def _get_header(
        self,
    ) -> Tuple[int, Optional[int], Optional[int], Optional[ResponseFlags]]:
        try:
            if self._noop_expected > 0:
                self._read_until_noop_header()

            return self._get_single_header()
        except Exception as e:
            _log.warning(f"Error reading header from socket in {self}")
            raise MemcacheError(f"Error reading header from socket: {e}") from e

    def get_response(
        self,
    ) -> Union[Value, Success, NotStored, Conflict, Miss]:
        (_, response_code, size, flags) = self._get_header()
        result: Union[Value, Success, NotStored, Conflict, Miss]
        try:
            if response_code == meta_memcache_socket.RESPONSE_VALUE:
                # Value response
                assert size is not None and flags is not None  # noqa: S101
                result = Value(size=size, flags=flags, value=None)
            elif response_code == meta_memcache_socket.RESPONSE_SUCCESS:
                # Stored or no value, return Success
                assert flags is not None  # noqa: S101
                result = Success(flags=flags)
            elif response_code == meta_memcache_socket.RESPONSE_NOT_STORED:
                # Value response, parse size and flags
                result = NOT_STORED
            elif response_code == meta_memcache_socket.RESPONSE_CONFLICT:
                # Already exists, not changed, CAS conflict
                result = CONFLICT
            elif response_code == meta_memcache_socket.RESPONSE_MISS:
                # Not Found, Miss.
                result = MISS
            else:
                raise MemcacheError(f"Unknown response: {response_code}")
        except Exception as e:
            _log.warning(
                f"Error parsing response header in {self}: "
                f"Response: {response_code}, size {size}, flags: {flags}"
            )
            raise MemcacheError("Error parsing response header") from e

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
