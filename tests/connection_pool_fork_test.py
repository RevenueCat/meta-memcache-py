import os
from typing import Callable
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.protocol import ServerVersion

pytestmark = pytest.mark.skipif(
    not hasattr(os, "fork"), reason="fork not available on this platform"
)


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


def test_pool_reset_after_fork(
    mocker: MockerFixture,
    mock_raw_socket: MagicMock,
) -> None:
    """After fork, child should get fresh connections, not shared FDs."""
    test_id_counter = 0

    def _make_unique_mock(*args, **kwargs):
        nonlocal test_id_counter
        test_id_counter += 1
        mock = MagicMock(spec=MemcacheSocket)
        mock.get_version.return_value = ServerVersion.STABLE
        mock._test_id = test_id_counter
        return mock

    mocker.patch(
        "meta_memcache.connection.pool.MemcacheSocket",
        side_effect=_make_unique_mock,
    )

    pool = ConnectionPool(
        server="test:0",
        socket_factory_fn=lambda: mock_raw_socket,
        initial_pool_size=2,
        max_pool_size=3,
        after_fork_initial_pool_size=1,
    )

    assert pool.get_counters().total_created == 2

    # Record parent connection ids
    parent_ids = set()
    with pool.get_connection() as conn:
        parent_ids.add(conn._test_id)
    with pool.get_connection() as conn:
        parent_ids.add(conn._test_id)

    def child_checks() -> None:
        # Counters should reflect only 1 fresh connection
        counters = pool.get_counters()
        assert counters.total_created == 1
        assert counters.available == 1

        # The child connection should have a different id
        with pool.get_connection() as child_conn:
            assert child_conn._test_id not in parent_ids

    _run_in_child(child_checks)

    # Parent pool should be unaffected
    with pool.get_connection() as parent_conn:
        assert parent_conn._test_id in parent_ids
    assert pool.get_counters().total_created == 2


def test_pool_reset_after_fork_default_no_connections(
    mock_raw_socket: MagicMock,
    mock_memcache_socket: MagicMock,
) -> None:
    """With default after_fork_initial_pool_size=0, child gets an empty pool."""
    pool = ConnectionPool(
        server="test:0",
        socket_factory_fn=lambda: mock_raw_socket,
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
