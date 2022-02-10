import logging
import socket
from typing import Iterable, List, Union

from meta_memcache.errors import MemcacheError
from meta_memcache.protocol import (
    ENDL,
    ENDL_LEN,
    MIN_HEADER_SIZE,
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
        self._version = version
        self._store_success_response_header: bytes = get_store_success_response_header(
            version
        )
        self._buf = bytearray(self._buffer_size)
        self._buf_view = memoryview(self._buf)
        self._noop_expected = 0

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
        self._read += read
        return read

    def _recv_endl_terminated_data(self, sized_buf: memoryview) -> int:
        """
        Received data into the given buffer. The size of the
        buffer has to be exactly the expected data size.

        The data in the socket has a termination mark (\r\n)
        that has to be read too. To avoid having to slice
        the data and cause yet another memory copy, we are
        using recvmsg and providing a separate, fixed size
        buffer for the termination mark.
        """
        msg_termination_buf = bytearray(ENDL_LEN)
        read: int = self._conn.recvmsg_into(
            [sized_buf, memoryview(msg_termination_buf)], flags=socket.MSG_WAITALL
        )
        if read != len(sized_buf) + ENDL_LEN or msg_termination_buf != ENDL:
            raise MemcacheError(
                f"Error parsing value: Expected {len(sized_buf)+ENDL_LEN} bytes, "
                f"terminated in \\r\\n, got {read} bytes: "
                f"{bytes(sized_buf)+bytes(msg_termination_buf)!r}"
            )
        return read - ENDL_LEN

    def _reset_buffer(self) -> None:
        """
        Reset buffer moving remaining bytes in it (if any)
        """
        remaining_data = self._read - self._pos
        if remaining_data:
            if self._pos <= self._buffer_size // 2:
                # Avoid moving memory if buffer still has
                # spare capacity for new responses. If the
                # whole buffer us used, we will just reset
                # the pointers and save a lot of memory
                # data copies
                return
            # pyre-ignore[6]
            self._buf_view[0:remaining_data] = self._buf_view[self._pos : self._read]
        self._pos = 0
        self._read = remaining_data

    def _recv_header(self) -> int:
        if self._read - self._pos < MIN_HEADER_SIZE:
            # No response in buffer
            self._recv_info_buffer()

        endl_pos = self._buf.find(ENDL, self._pos, self._read)
        if endl_pos < 0:
            # No ENDL found in buffer
            old_read = self._read
            self._recv_info_buffer()
            endl_pos = self._buf.find(ENDL, old_read, self._read)

            if endl_pos < 0:
                raise MemcacheError(
                    "Bad response. Socket might have closed unexpectedly"
                )

        return endl_pos

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
                if f := int_flags_values.get(flag):
                    # pyre-ignore[6]
                    success.int_flags[f] = int(chunk[1:])
                elif f := token_flags_values.get(flag):
                    success.token_flags[f] = bytes(chunk[1:])
                else:
                    _log.warning(f"Unrecognized flag {bytes(chunk)}")

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

    def _get_header(self) -> List[memoryview]:
        endl_pos = self._recv_header()
        header = self._buf_view[self._pos : endl_pos]
        self._pos = endl_pos + ENDL_LEN
        return self._tokenize_header(header)

    def sendall(self, data: bytes, with_noop: bool = False) -> None:
        if with_noop:
            self._noop_expected += 1
            data += NOOP
        self._conn.sendall(data)

    def _read_until_noop_header(self) -> None:
        while self._noop_expected > 0:
            response_code, *_chunks = self._get_header()
            if response_code == b"MN":
                self._noop_expected -= 1

    def get_response(
        self,
    ) -> Union[Value, Success, NotStored, Conflict, Miss]:
        if self._noop_expected > 0:
            self._read_until_noop_header()

        header = self._get_header()
        response_code, *chunks = header
        try:
            if response_code == b"VA":
                # Value response, parse size and flags
                # pyre-ignore[6]
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
                raise MemcacheError(f"Unknown response: {bytes(response_code)}")
        except Exception as e:
            raise MemcacheError(
                f'Error parsing response header {b" ".join(header)}'
            ) from e

        self._reset_buffer()
        return result

    def get_value(self, size: int) -> Union[bytearray, memoryview]:
        """
        Get data value from the buffer and/or socket if hasn't been
        read fully.
        """
        data_in_buf = self._read - self._pos
        missing_data_size = size - data_in_buf
        if missing_data_size > 0 and self._read < len(self._buf):
            # Missing data, but still space in buffer, so read more
            self._recv_info_buffer()

        data_in_buf = self._read - self._pos
        missing_data_size = size - data_in_buf
        if missing_data_size > 0:
            # Value is greater than buffer:
            # - generate new bytearray of the total size
            # - copy data in buffer into new bytearray
            # - read the remaining data from socket and combine it
            data = bytearray(size)
            view = memoryview(data)
            # pyre-ignore[6]
            view[:data_in_buf] = self._buf_view[self._pos : self._read]
            read = self._recv_endl_terminated_data(view[data_in_buf:])
            if size != data_in_buf + read:
                raise MemcacheError(
                    f"Error parsing value. Expected {size} bytes, "
                    f"got {data_in_buf}+{read}"
                )
            # The whole buffer was used
            self._pos = self._read
        else:
            # Value is within buffer, slice it
            data_end = self._pos + size
            data = self._buf_view[self._pos : data_end]
            # Advance pos to data end
            self._pos = data_end
            # Ensure the data is correctly terminated
            if self._read < self._pos + ENDL_LEN:
                # the buffer ended half-way the ENDL termination
                # message, we need to read more from the wire.
                self._reset_buffer()
                self._recv_info_buffer()
                assert self._read >= self._pos + ENDL_LEN  # noqa: S101
            if self._buf_view[self._pos : self._pos + ENDL_LEN] != ENDL:
                raise MemcacheError("Error parsing value. Data doesn't end in \\r\\n")
            self._pos += ENDL_LEN

        self._reset_buffer()
        if len(data) != size:
            raise MemcacheError(
                f"Error parsing value. Expected {size} bytes, got {len(data)}"
            )
        return data
