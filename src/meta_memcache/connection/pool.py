import itertools
import logging
import os
import socket
import time
import weakref
from collections import deque
from contextlib import contextmanager
from typing import Callable, Deque, Generator, NamedTuple, Optional

from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.errors import MemcacheServerError, ServerMarkedDownError
from meta_memcache.protocol import ServerVersion
from meta_memcache.settings import DEFAULT_MARK_DOWN_PERIOD_S, DEFAULT_READ_BUFFER_SIZE

_log: logging.Logger = logging.getLogger(__name__)


def _get_pool_registry() -> weakref.WeakSet["ConnectionPool"]:
    """Get the pool registry, registering the at-fork handler on first call.

    Returns a WeakSet that tracks all ConnectionPool instances. On first call,
    also registers an os.register_at_fork() handler so that after fork(), all
    pooled connections are closed in the child process. This prevents parent
    and child from sharing sockets, which causes protocol-level corruption.

    Call _get_pool_registry().add(pool) to register a pool for fork safety.
    """
    global _pool_registry
    if _pool_registry is None:
        _pool_registry = weakref.WeakSet()
        if hasattr(os, "register_at_fork"):
            registry = _pool_registry
            os.register_at_fork(
                after_in_child=lambda: _reset_pools_after_fork(registry)
            )
    return _pool_registry


_pool_registry: Optional[weakref.WeakSet["ConnectionPool"]] = None


def _reset_pools_after_fork(
    registry: weakref.WeakSet["ConnectionPool"],
) -> None:
    for pool in list(registry):
        pool._reset_after_fork()


class PoolCounters(NamedTuple):
    # Available connections in the pool, ready to use
    available: int
    # The # of connections active, currently in use, out of the pool
    active: int
    # Current stablished connections (available + active)
    stablished: int
    # Total # of connections created. If this keeps growing
    # might meen the pool size is too small and we are
    # constantly needing to create new connections:
    total_created: int
    # Total # of connection or socket errors
    total_errors: int


class ConnectionPool:
    def __init__(
        self,
        server: str,
        socket_factory_fn: Callable[[], socket.socket],
        initial_pool_size: int,
        max_pool_size: int,
        mark_down_period_s: float = DEFAULT_MARK_DOWN_PERIOD_S,
        read_buffer_size: int = DEFAULT_READ_BUFFER_SIZE,
        version: ServerVersion = ServerVersion.STABLE,
        after_fork_initial_pool_size: int = 0,
    ) -> None:
        self.server = server
        self._socket_factory_fn = socket_factory_fn
        self._initial_pool_size: int = min(initial_pool_size, max_pool_size)
        self._after_fork_initial_pool_size: int = min(
            after_fork_initial_pool_size, max_pool_size
        )
        self._max_pool_size = max_pool_size
        self._mark_down_period_s = mark_down_period_s
        # We don't use maxlen because deque will evict the first element when
        # appending one over maxlen, and we won't be closing the connection
        # proactively, relying on GC instead. We use a soft max limit, after
        # all is not that critical to respect the number of connections
        # exactly.
        self._pool: Deque[MemcacheSocket] = deque()
        self._read_buffer_size = read_buffer_size
        self._version = version
        self._init_pool(self._initial_pool_size)
        _get_pool_registry().add(self)

    def _init_pool(self, initial_size: int) -> None:
        """Reset counters and pre-populate the pool with initial connections."""
        self._created_counter: itertools.count[int] = itertools.count(start=1)
        self._created = 0
        self._errors_counter: itertools.count[int] = itertools.count(start=1)
        self._errors = 0
        self._destroyed_counter: itertools.count[int] = itertools.count(start=1)
        self._destroyed = 0
        self._marked_down_until: Optional[float] = None
        for _ in range(initial_size):
            try:
                self._pool.append(self._create_connection())
            except MemcacheServerError:
                pass

    def _reset_after_fork(self) -> None:
        """Reset pool state in child process after fork.

        Closes all inherited connections to avoid sharing sockets with
        the parent process, then re-initializes with fresh connections.
        """
        while self._pool:
            conn = self._pool.popleft()
            try:
                conn.close()
            except Exception:
                pass
        self._init_pool(self._after_fork_initial_pool_size)

    def get_counters(self) -> PoolCounters:
        available = len(self._pool)
        total_created, total_destroyed = self._created, self._destroyed
        stablished = total_created - total_destroyed
        active = available - stablished

        return PoolCounters(
            available=available,
            active=active,
            stablished=stablished,
            total_created=total_created,
            total_errors=self._errors,
        )

    def _create_connection(self) -> MemcacheSocket:
        if marked_down_until := self._marked_down_until:
            if time.time() < marked_down_until:
                raise ServerMarkedDownError(
                    self.server, f"Server marked down: {self.server}"
                )
            self._marked_down_until = None

        try:
            conn = self._socket_factory_fn()
        except Exception as e:
            _log.warning(
                "Error connecting to memcache",
                exc_info=True,
            )
            self._errors = next(self._errors_counter)
            self._marked_down_until = time.time() + self._mark_down_period_s
            raise ServerMarkedDownError(
                self.server, f"Server marked down: {self.server}"
            ) from e

        self._created = next(self._created_counter)
        return MemcacheSocket(conn, self._read_buffer_size, version=self._version)

    def _discard_connection(self, conn: MemcacheSocket, error: bool = False) -> None:
        try:
            conn.close()
        except Exception:  # noqa: S110
            pass
        if error:
            self._errors = next(self._errors_counter)
        self._destroyed = next(self._destroyed_counter)

    @contextmanager
    def get_connection(self) -> Generator[MemcacheSocket, None, None]:
        conn = self.pop_connection()
        try:
            yield conn
        except Exception as e:
            self.release_connection(conn, error=True)
            raise MemcacheServerError(self.server, "Memcache error") from e
        else:
            self.release_connection(conn, error=False)

    def pop_connection(self) -> MemcacheSocket:
        try:
            return self._pool.popleft()
        except IndexError:
            return self._create_connection()

    def release_connection(self, conn: MemcacheSocket, error: bool) -> None:
        if error:
            # Errors, assume connection is in bad state
            _log.warning(
                "Error during cache conn context (discarding connection)",
                exc_info=True,
            )
            self._discard_connection(conn, error=True)
        else:
            if len(self._pool) < self._max_pool_size:
                # If there is a race, the  deque might end with more than
                # self._max_pool_size but it is not a big deal, we want this
                # to be a soft limit.
                self._pool.append(conn)
            else:
                self._discard_connection(conn)
