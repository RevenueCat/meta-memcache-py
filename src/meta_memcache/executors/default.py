import logging
from typing import Callable, Dict, List, Optional, Tuple

from meta_memcache_socket import (
    RequestFlags,
    build_meta_get,
    build_meta_delete,
    build_meta_set,
    build_meta_arithmetic,
)

from meta_memcache.base.base_serializer import BaseSerializer
from meta_memcache.configuration import default_key_encoder
from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.errors import MemcacheServerError
from meta_memcache.events.write_failure_event import WriteFailureEvent
from meta_memcache.protocol import (
    ENDL,
    Key,
    MaybeValue,
    MemcacheResponse,
    MetaCommand,
    Miss,
    NotStored,
    ResponseFlags,
    ServerVersion,
    Success,
    Value,
    ValueContainer,
)

_log: logging.Logger = logging.getLogger(__name__)


class DefaultExecutor:
    def __init__(
        self,
        serializer: BaseSerializer,
        key_encoder_fn: Callable[[Key], bytes] = default_key_encoder,
        raise_on_server_error: bool = True,
        touch_ttl_to_consider_write_failure: Optional[int] = 50,
    ) -> None:
        self._serializer = serializer
        self._key_encoder_fn = key_encoder_fn
        self._raise_on_server_error = raise_on_server_error
        self._touch_ttl_to_consider_write_failure = touch_ttl_to_consider_write_failure
        self.on_write_failure = WriteFailureEvent()

    def _build_cmd(
        self,
        command: MetaCommand,
        key: Key,
        size: Optional[int] = None,
        flags: Optional[RequestFlags] = None,
        version: ServerVersion = ServerVersion.STABLE,
    ) -> bytes:
        encoded_key = self._key_encoder_fn(key)
        if command == MetaCommand.META_GET:
            return build_meta_get(encoded_key, flags)
        elif command == MetaCommand.META_SET:
            legazy_size_format = version == ServerVersion.AWS_1_6_6
            return build_meta_set(encoded_key, size, flags, legazy_size_format)
        elif command == MetaCommand.META_DELETE:
            return build_meta_delete(encoded_key, flags)
        elif command == MetaCommand.META_ARITHMETIC:
            return build_meta_arithmetic(encoded_key, flags)

    def _prepare_serialized_value_and_flags(
        self,
        value: ValueContainer,
        flags: Optional[RequestFlags],
    ) -> Tuple[Optional[bytes], RequestFlags]:
        encoded_value = self._serializer.serialize(value.value)
        flags = flags if flags is not None else RequestFlags()
        flags.client_flag = encoded_value.encoding_id
        return encoded_value.data, flags

    def _is_a_write_failure(
        self, command: MetaCommand, flags: Optional[RequestFlags]
    ) -> bool:
        if command in (
            MetaCommand.META_DELETE,
            MetaCommand.META_SET,
        ):
            return True
        if (
            self._touch_ttl_to_consider_write_failure is not None
            and command == MetaCommand.META_GET
            and (touch_ttl := (flags.cache_ttl if flags else None))
            and 0 < touch_ttl <= self._touch_ttl_to_consider_write_failure
        ):
            return True
        return False

    def exec_on_pool(
        self,
        pool: ConnectionPool,
        command: MetaCommand,
        key: Key,
        value: MaybeValue,
        flags: Optional[RequestFlags],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
    ) -> MemcacheResponse:
        cmd_value, flags = (
            (None, flags)
            if value is None
            else self._prepare_serialized_value_and_flags(value, flags)
        )
        try:
            conn = pool.pop_connection()
            error = False
            try:
                self._conn_send_cmd(
                    conn,
                    command=command,
                    key=key,
                    value=cmd_value,
                    flags=flags,
                )
                return self._conn_recv_response(conn, flags=flags)
            except Exception as e:
                error = True
                raise MemcacheServerError(pool.server, "Memcache error") from e
            finally:
                pool.release_connection(conn, error=error)
        except MemcacheServerError:
            if track_write_failures and self._is_a_write_failure(command, flags):
                self.on_write_failure(key)
            raise_on_server_error = (
                raise_on_server_error
                if raise_on_server_error is not None
                else self._raise_on_server_error
            )
            if raise_on_server_error:
                raise
            if command == MetaCommand.META_GET:
                return Miss()
            else:
                return NotStored()

    def exec_multi_on_pool(  # noqa: C901
        self,
        pool: ConnectionPool,
        command: MetaCommand,
        key_values: List[Tuple[Key, MaybeValue]],
        flags: Optional[RequestFlags],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
    ) -> Dict[Key, MemcacheResponse]:
        results: Dict[Key, MemcacheResponse] = {}
        try:
            conn = pool.pop_connection()
            error = False
            try:
                # with pool.get_connection() as conn:
                for key, value in key_values:
                    cmd_value, flags = (
                        (None, flags)
                        if value is None
                        else self._prepare_serialized_value_and_flags(value, flags)
                    )

                    self._conn_send_cmd(
                        conn,
                        command=command,
                        key=key,
                        value=cmd_value,
                        flags=flags,
                    )
                for key, _ in key_values:
                    results[key] = self._conn_recv_response(conn, flags=flags)
            except Exception as e:
                error = True
                raise MemcacheServerError(pool.server, "Memcache error") from e
            finally:
                pool.release_connection(conn, error=error)
        except MemcacheServerError:
            if track_write_failures and self._is_a_write_failure(command, flags):
                for key, _ in key_values:
                    self.on_write_failure(key)
            raise_on_server_error = (
                raise_on_server_error
                if raise_on_server_error is not None
                else self._raise_on_server_error
            )
            if raise_on_server_error:
                raise
            failure_result = Miss() if command == MetaCommand.META_GET else NotStored()
            for key, _ in key_values:
                if key not in results:
                    results[key] = failure_result

        return results

    def _conn_send_cmd(
        self,
        conn: MemcacheSocket,
        command: MetaCommand,
        key: Key,
        value: Optional[bytes] = None,
        flags: Optional[RequestFlags] = None,
    ) -> None:
        """
        Execute command on a connection
        """
        cmd = self._build_cmd(
            command,
            key,
            size=len(value) if value is not None else None,
            flags=flags,
            version=conn.get_version(),
        )
        # write meta commands with NOREPLY can potentially return errors
        # they are not fully silent, so we need to add a no-op to the wire.
        with_noop = (
            command != MetaCommand.META_GET and flags is not None and flags.no_reply
        )

        if value:
            conn.sendall(cmd + value + ENDL, with_noop=with_noop)
        else:
            conn.sendall(cmd, with_noop=with_noop)

    def _conn_recv_response(
        self,
        conn: MemcacheSocket,
        flags: Optional[RequestFlags] = None,
    ) -> MemcacheResponse:
        """
        Read response on a connection
        """
        if flags and flags.no_reply:
            return Success(flags=ResponseFlags())
        result = conn.get_response()
        if isinstance(result, Value):
            data = conn.get_value(result.size)
            if result.size > 0:
                encoding_id = result.flags.client_flag or 0
                try:
                    result.value = self._serializer.unserialize(data, encoding_id)
                except Exception:
                    _log.exception(
                        f"Error unserializing value {data} "
                        f"with encoding id: {encoding_id}"
                    )
                    result = Miss()

        return result
