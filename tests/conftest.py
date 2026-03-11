from unittest.mock import MagicMock

import pytest

from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.protocol import ServerVersion


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
    mocker.patch(
        "meta_memcache.connection.pool.MemcacheSocket",
        side_effect=lambda *args, **kwargs: mock,
    )
    return mock
