import itertools
import logging
import socket
import time
from collections import deque
from contextlib import contextmanager
from typing import Callable, Deque, Generator, NamedTuple, Optional

from meta_memcache.base.memcache_socket import MemcacheSocket
from meta_memcache.errors import MemcacheServerError, ServerMarkedDownError
from meta_memcache.protocol import ServerVersion
from meta_memcache.settings import DEFAULT_MARK_DOWN_PERIOD_S, DEFAULT_READ_BUFFER_SIZE

_log: logging.Logger = logging.getLogger(__name__)


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

class ConnectionHolder(NamedTuple):
    connection: MemcacheSocket
    connection_error_count: int

class ConnectionPool:
    def __init__(
        self,
        server: str,
        socket_factory_fn: Callable[[], socket.socket],
        initial_pool_size: int,
        max_pool_size: int,
        mark_down_period_s: float = DEFAULT_MARK_DOWN_PERIOD_S,
        mark_down_period_errors_required_to_trigger: int = 1,
        errors_required_to_discard_connection: int = 1,
        read_buffer_size: int = DEFAULT_READ_BUFFER_SIZE,
        version: ServerVersion = ServerVersion.STABLE,
        new_connection_spacing_s: Optional[float] = 0,
    ) -> None:
        self.server = server
        self._socket_factory_fn = socket_factory_fn
        self._initial_pool_size: int = min(initial_pool_size, max_pool_size)
        self._max_pool_size = max_pool_size
        self._mark_down_period_s = mark_down_period_s
        self._mark_down_period_errors_required_to_trigger = mark_down_period_errors_required_to_trigger
        self._last_error_count_when_marked_down = 0
        self._errors_required_to_discard_connection = errors_required_to_discard_connection
        self._created_counter: itertools.count[int] = itertools.count(start=1)
        self._created = 0
        self._errors_counter: itertools.count[int] = itertools.count(start=1)
        self._errors = 0
        self._destroyed_counter: itertools.count[int] = itertools.count(start=1)
        self._destroyed = 0
        self._marked_down_until: Optional[float] = None
        # We don't use maxlen because deque will evict the first element when
        # appending one over maxlen, and we won't be closing the connection
        # proactively, relying on GC instead. We use a soft max limit, after
        # all is not that critical to respect the number of connections
        # exactly.
        self._pool: Deque[ConnectionHolder] = deque()
        self._read_buffer_size = read_buffer_size
        self._version = version
        self._new_connection_spacing_s = new_connection_spacing_s
        for _ in range(self._initial_pool_size):
            try:
                self._pool.append(self._create_connection())
            except MemcacheServerError:
                pass

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

    def _create_connection(self) -> ConnectionHolder:
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

            if self._errors >= self._last_error_count_when_marked_down + self._mark_down_period_errors_required_to_trigger:
                self._marked_down_until = time.time() + self._mark_down_period_s
                raise ServerMarkedDownError(
                    self.server, f"Server marked down: {self.server}"
                ) from e
            else:
                raise e

        self._created = next(self._created_counter)
        return ConnectionHolder(connection=MemcacheSocket(conn, self._read_buffer_size, version=self._version), connection_error_count=0)

    def _discard_connection(self, conn_holder: ConnectionHolder, error: bool = False) -> None:
        try:
            conn_holder.connection.close()
        except Exception:  # noqa: S110
            pass
        if error:
            self._errors = next(self._errors_counter)
        self._destroyed = next(self._destroyed_counter)

    @contextmanager
    def get_connection(self) -> Generator[MemcacheSocket, None, None]:
        try:
            conn_holder = self._pool.popleft()
        except IndexError:
            conn_holder = None

        if conn_holder is None:
            time.sleep(self._new_connection_spacing_s)            
            conn_holder = self._create_connection()

        should_discard_from_error = False
        try:
            yield conn_holder.connection
        except Exception as e:
            conn_holder.connection_error_count += 1

            should_discard_from_error = conn_holder.connection_error_count >= self._errors_required_to_discard_connection

            if should_discard_from_error:
                # Errors, assume connection is in bad state
                _log.warning(
                    "Error during cache conn context (discarding connection)",
                    exc_info=True,
                )
                self._discard_connection(conn_holder, error=True)

            raise MemcacheServerError(self.server, "Memcache error") from e
        
        if not should_discard_from_error:
            if len(self._pool) < self._max_pool_size:
                # If there is a race, the  deque might end with more than
                # self._max_pool_size but it is not a big deal, we want this
                # to be a soft limit.
                self._pool.append(conn_holder)
            else:
                self._discard_connection(conn_holder)
