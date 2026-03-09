import os
import socket
from typing import Callable, Generator

import pytest

from meta_memcache.connection.pool import ConnectionPool

pytestmark = pytest.mark.skipif(
    not hasattr(os, "fork"), reason="fork not available on this platform"
)


@pytest.fixture
def socket_factory() -> Generator[Callable[[], socket.socket], None, None]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    port = listener.getsockname()[1]

    def factory() -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", port))
        return s

    yield factory
    listener.close()


def _run_in_child(fn: Callable[[], None]) -> None:
    """Fork, run fn in the child, and assert it succeeded."""
    r_fd, w_fd = os.pipe()
    pid = os.fork()

    if pid == 0:
        os.close(r_fd)
        try:
            fn()
            os.write(w_fd, b"OK")
        except (Exception, AssertionError) as e:
            os.write(w_fd, f"ERROR:{e}".encode())
        finally:
            os.close(w_fd)
            os._exit(0)
    else:
        os.close(w_fd)
        _, status = os.waitpid(pid, 0)
        data = b""
        while chunk := os.read(r_fd, 1024):
            data += chunk
        os.close(r_fd)
        result = data.decode()
        assert result == "OK", f"Child failed: {result}"


def test_pool_reset_after_fork(socket_factory: Callable[[], socket.socket]) -> None:
    """After fork, child should get fresh connections, not shared FDs."""
    pool = ConnectionPool(
        server="test:0",
        socket_factory_fn=socket_factory,
        initial_pool_size=2,
        max_pool_size=3,
        after_fork_initial_pool_size=1,
    )

    assert pool.get_counters().total_created == 2

    # Remember the client-side ephemeral ports of the parent's connections
    parent_ports = set()
    with pool.get_connection() as conn:
        parent_ports.add(conn._conn.getsockname()[1])
    with pool.get_connection() as conn:
        parent_ports.add(conn._conn.getsockname()[1])

    def child_checks() -> None:
        # Counters should reflect only 1 fresh connection
        assert pool.get_counters().total_created == 1

        # The connection should be a new one with a different ephemeral port
        with pool.get_connection() as child_conn:
            child_port = child_conn._conn.getsockname()[1]
            assert child_port not in parent_ports

    _run_in_child(child_checks)

    # Parent pool should be unaffected
    with pool.get_connection() as parent_conn:
        assert parent_conn._conn.getsockname()[1] in parent_ports
    assert pool.get_counters().total_created == 2


def test_pool_reset_after_fork_default_no_connections(
    socket_factory: Callable[[], socket.socket],
) -> None:
    """With default after_fork_initial_pool_size=0, child gets an empty pool."""
    pool = ConnectionPool(
        server="test:0",
        socket_factory_fn=socket_factory,
        initial_pool_size=1,
        max_pool_size=3,
    )

    assert pool.get_counters().total_created == 1

    def child_checks() -> None:
        counters = pool.get_counters()
        assert counters.total_created == 0
        assert counters.available == 0

    _run_in_child(child_checks)

    # Parent unaffected
    assert pool.get_counters().total_created == 1
