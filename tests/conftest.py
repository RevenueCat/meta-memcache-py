import collections
import queue
import socket as sock
import threading
from unittest.mock import MagicMock

import pytest

from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.protocol import Miss, ResponseFlags, ServerVersion, Success

# Response constants
HD = b"HD\r\n"
MN = b"MN\r\n"
EN = b"EN\r\n"
NS = b"NS\r\n"


@pytest.fixture
def mock_raw_socket(mocker) -> MagicMock:
    """Mock the socket module in meta_memcache.configuration.

    Returns the mock raw socket instance (shared across all socket.socket() calls).
    Useful for tests that need to control connect behavior (e.g. side_effect to
    simulate connection failures).
    """
    socket_mod = mocker.patch("meta_memcache.configuration.socket", autospec=True)
    return socket_mod.socket()


@pytest.fixture
def mock_memcache_socket(mocker, mock_raw_socket) -> MagicMock:
    """A single shared mock MemcacheSocket, with socket creation already mocked.

    Patches both the socket module and MemcacheSocket so that all connection
    pools use this mock. Use this fixture to assert on sendall, get_response, etc.
    """
    mock = MagicMock(spec=MemcacheSocket)
    mock.get_version.return_value = ServerVersion.STABLE
    _success = Success(flags=ResponseFlags())
    mock.meta_get.return_value = Miss()
    mock.meta_multiget.side_effect = lambda keys, request_flags=None: [
        Miss() for _ in keys
    ]
    mock.meta_set.return_value = _success
    mock.meta_delete.return_value = _success
    mock.meta_arithmetic.return_value = _success
    mocker.patch(
        "meta_memcache.connection.pool.MemcacheSocket",
        side_effect=lambda *args, **kwargs: mock,
    )
    return mock


class WireSocket:
    """A real MemcacheSocket backed by a socket pair for wire-level testing.

    A background thread reads what the client sends, records it, and auto-responds.
    Responses come from the queue (if any) or the default_response.

    Use read_wire() to inspect the raw bytes that were sent.
    Use queue_response() to enqueue specific responses for upcoming commands.
    """

    def __init__(self, version: int = ServerVersion.STABLE) -> None:
        self.client_sock, self.peer_sock = sock.socketpair()
        self.memcache_socket = MemcacheSocket(self.client_sock, version=version)
        self._wire_deque: collections.deque[bytes] = collections.deque()
        self._data_event = threading.Event()
        self._response_queue: queue.Queue[bytes] = queue.Queue()
        self.default_response: bytes = HD
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._responder, daemon=True)
        self._thread.start()

    def queue_response(self, *responses: bytes) -> None:
        """Enqueue one or more responses for upcoming commands."""
        for r in responses:
            self._response_queue.put(r)

    def _next_response(self) -> bytes:
        try:
            return self._response_queue.get_nowait()
        except queue.Empty:
            return self.default_response

    def _send_response(self, response: bytes) -> bool:
        try:
            self.peer_sock.sendall(response)
            return True
        except OSError:
            return False

    def _responder(self) -> None:
        self.peer_sock.settimeout(0.5)
        buf = b""
        while not self._stop.is_set():
            try:
                data = self.peer_sock.recv(65536)
            except (sock.timeout, TimeoutError):
                continue
            except OSError:
                break
            if not data:
                break
            self._wire_deque.append(data)
            self._data_event.set()
            buf += data
            while b"\r\n" in buf:
                line, buf = buf.split(b"\r\n", 1)
                if line == b"mn":
                    if not self._send_response(MN):
                        return
                elif line.startswith((b"mg ", b"md ", b"ma ")):
                    if not self._send_response(self._next_response()):
                        return
                elif line.startswith(b"ms "):
                    # meta set header: reply now, next \r\n is the value (no reply needed)
                    if not self._send_response(self._next_response()):
                        return

    def read_wire(self) -> bytes:
        """Pop and return all wire bytes collected so far.

        Uses an event to wait for the responder thread to process
        any pending data (needed for fire-and-forget sends like no_reply
        where the client doesn't block on recv).
        """
        self._data_event.wait(timeout=0.5)
        self._data_event.clear()
        chunks = []
        while self._wire_deque:
            chunks.append(self._wire_deque.popleft())
        return b"".join(chunks)

    def assert_wire(self, *expected_commands: bytes) -> None:
        """Read wire and assert it matches expected command(s) exactly."""
        wire = self.read_wire()
        expected = b"".join(expected_commands)
        assert wire == expected, (
            f"wire mismatch:\n  got:  {wire!r}\n  want: {expected!r}"
        )

    def close(self) -> None:
        self._stop.set()
        self.client_sock.close()
        self.peer_sock.close()
        self._thread.join(timeout=2)


@pytest.fixture
def wire_memcache_socket():
    """A WireSocket for direct wire-level testing (not patched into pools)."""
    ws = WireSocket()
    yield ws
    ws.close()


@pytest.fixture
def wire_memcache_socket_1_6_6():
    """A WireSocket with AWS 1.6.6 version (not patched into pools)."""
    ws = WireSocket(version=ServerVersion.AWS_1_6_6)
    yield ws
    ws.close()


@pytest.fixture
def wire_socket(mocker, mock_raw_socket):
    """Fixture providing a real MemcacheSocket on a socket pair.

    The MemcacheSocket is patched into the connection pool so all pools
    use it. Use wire_socket.read_wire() to inspect the raw bytes sent.
    """
    ws = WireSocket()
    mocker.patch(
        "meta_memcache.connection.pool.MemcacheSocket",
        side_effect=lambda *args, **kwargs: ws.memcache_socket,
    )
    yield ws
    ws.close()
