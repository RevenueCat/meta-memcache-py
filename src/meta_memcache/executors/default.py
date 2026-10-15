import logging
import time
from typing import Callable, Dict, List, Literal, Optional, Tuple, Union

from meta_memcache_socket import RequestFlags

from meta_memcache.base.base_serializer import BaseSerializer
from meta_memcache.connection.memcache_socket import MemcacheSocket
from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.errors import MemcacheServerError, UserDataError
from meta_memcache.events.write_failure_event import WriteFailureEvent
from meta_memcache.protocol import (
    MISS_DUE_TO_ERROR,
    NOT_STORED_DUE_TO_ERROR,
    Key,
    MaybeValue,
    MemcacheResponse,
    MetaCommand,
    Miss,
    ResponseFlags,
    Success,
    Value,
    ValueContainer,
)
from meta_memcache.stats import (
    CacheStats,
    StatsCallback,
    StatsSampler,
    sample_weight,
)

_log: logging.Logger = logging.getLogger(__name__)

# Commands that write (or attempt to write) a value. These are pipelined one by
# one, unlike gets, which have dedicated multi-get support in the protocol.
WriteMetaCommand = Literal[
    MetaCommand.META_SET,
    MetaCommand.META_DELETE,
    MetaCommand.META_ARITHMETIC,
]


class _OperationTimes:
    """
    Where a sampled operation spent its time, for its stats. The clock starts
    when it is created, and each phase is a lap: the time since the previous
    one, so the phases share their reads of the clock and add up to the
    operation.
    """

    __slots__ = (
        "start_ns",
        "serialize_ns",
        "pool_ns",
        "network_ns",
        "deserialize_ns",
        "_lap_start_ns",
    )

    def __init__(self) -> None:
        self.start_ns = self._lap_start_ns = time.time_ns()
        self.serialize_ns = 0
        self.pool_ns = 0
        self.network_ns = 0
        self.deserialize_ns = 0

    # They return the lap so they can be short-circuited on an unsampled
    # operation: `times and times.lap_pool()`
    def lap_serialize(self) -> int:
        self.serialize_ns = self._lap_ns()
        return self.serialize_ns

    def lap_pool(self) -> int:
        self.pool_ns = self._lap_ns()
        return self.pool_ns

    def lap_network(self) -> int:
        self.network_ns = self._lap_ns()
        return self.network_ns

    def lap_deserialize(self) -> int:
        self.deserialize_ns = self._lap_ns()
        return self.deserialize_ns

    def _lap_ns(self) -> int:
        now_ns = time.time_ns()
        lap_ns = now_ns - self._lap_start_ns
        self._lap_start_ns = now_ns
        return lap_ns


class DefaultExecutor:
    def __init__(
        self,
        serializer: BaseSerializer,
        key_encoder_fn: Optional[Callable[[Key], bytes]] = None,
        raise_on_server_error: bool = True,
        touch_ttl_to_consider_write_failure: Optional[int] = 50,
        stats_callback: Optional[StatsCallback] = None,
        stats_sampler: Optional[StatsSampler] = None,
    ) -> None:
        self._serializer = serializer
        self._key_encoder_fn = key_encoder_fn
        self._raise_on_server_error = raise_on_server_error
        self._touch_ttl_to_consider_write_failure = touch_ttl_to_consider_write_failure
        self._stats_callback = stats_callback
        self._stats_sampler = stats_sampler
        self.on_write_failure = WriteFailureEvent()

    def _prepare_serialized_value_and_flags(
        self,
        key: Key,
        value: ValueContainer,
        flags: Optional[RequestFlags],
    ) -> Tuple[Optional[bytes], RequestFlags]:
        encoded_value = self._serializer.serialize(key, value.value)
        if flags is None:
            flags = RequestFlags(client_flag=encoded_value.encoding_id)
        else:
            flags = flags.replace(client_flag=encoded_value.encoding_id)
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

    def _conn_exec_cmd(
        self,
        conn: MemcacheSocket,
        command: MetaCommand,
        key: Key,
        value: Optional[bytes] = None,
        flags: Optional[RequestFlags] = None,
    ) -> MemcacheResponse:
        """
        Execute a blocking command on a connection (send + recv).
        The blocking meta_* methods handle no_reply internally.
        The response is left for the caller to process, once the
        connection is back in the pool.
        """
        wire_key: Union[str, bytes] = (
            self._key_encoder_fn(key) if self._key_encoder_fn else key.key
        )
        if command == MetaCommand.META_GET:
            result = conn.meta_get(wire_key, flags)
        elif command == MetaCommand.META_SET:
            assert value is not None
            result = conn.meta_set(wire_key, value, flags)
        elif command == MetaCommand.META_DELETE:
            result = conn.meta_delete(wire_key, flags)
        elif command == MetaCommand.META_ARITHMETIC:
            result = conn.meta_arithmetic(wire_key, flags)
        else:
            raise ValueError(f"Unknown command: {command}")
        return result

    def _process_response(self, result: MemcacheResponse) -> MemcacheResponse:
        if isinstance(result, Value):
            if result.size > 0:
                encoding_id = result.flags.client_flag or 0
                try:
                    result.value = self._serializer.unserialize(
                        result.value, encoding_id
                    )
                except UserDataError:
                    """ Don't log user data errors when unserializing """
                    result = Miss()
                except Exception:
                    _log.exception(
                        f"Error unserializing value {result.value} "
                        f"with encoding id: {encoding_id}"
                    )
                    result = Miss()
        return result

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
        callback = self._stats_callback
        weight = None if callback is None else sample_weight(self._stats_sampler)
        if weight is None:
            return self._exec_on_pool(
                pool,
                command,
                key,
                value,
                flags,
                track_write_failures,
                raise_on_server_error,
            )
        times = _OperationTimes()
        response: Optional[MemcacheResponse] = None
        error: Optional[Exception] = None
        try:
            response = self._exec_on_pool(
                pool,
                command,
                key,
                value,
                flags,
                track_write_failures,
                raise_on_server_error,
                times,
            )
            return response
        except Exception as e:
            error = e
            raise
        finally:
            stats = CacheStats(
                command=command,
                keys=[key],
                size=response.size if isinstance(response, Value) else 0,
                server=pool.server,
                flags=flags,
                responses=None if response is None else {key: response},
                start_time_ns=times.start_ns,
                duration_ns=time.time_ns() - times.start_ns,
                error=error,
                weight=weight,
                serialize_duration_ns=times.serialize_ns,
                pool_duration_ns=times.pool_ns,
                network_duration_ns=times.network_ns,
                deserialize_duration_ns=times.deserialize_ns,
            )
            try:
                # There is a weight, so there is a callback
                callback(stats)  # type: ignore[misc]
            except Exception:
                _log.warning("Error reporting cache stats", exc_info=True)

    def _exec_on_pool(
        self,
        pool: ConnectionPool,
        command: MetaCommand,
        key: Key,
        value: MaybeValue,
        flags: Optional[RequestFlags],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
        times: Optional[_OperationTimes] = None,
    ) -> MemcacheResponse:
        cmd_value: Optional[bytes] = None
        if value is not None:
            cmd_value, flags = self._prepare_serialized_value_and_flags(
                key, value, flags
            )
            times and times.lap_serialize()
        try:
            conn = pool.pop_connection()
            times and times.lap_pool()
            error = False
            try:
                response = self._conn_exec_cmd(
                    conn,
                    command=command,
                    key=key,
                    value=cmd_value,
                    flags=flags,
                )
            except Exception as e:
                error = True
                raise MemcacheServerError(pool.server, "Memcache error") from e
            finally:
                pool.release_connection(conn, error=error)
                times and times.lap_network()
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
                return MISS_DUE_TO_ERROR
            else:
                return NOT_STORED_DUE_TO_ERROR

        response = self._process_response(response)
        times and times.lap_deserialize()
        return response

    def _exec_multi_get_on_pool(
        self,
        pool: ConnectionPool,
        keys: List[Key],
        flags: Optional[RequestFlags],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
        times: Optional[_OperationTimes] = None,
    ) -> Dict[Key, MemcacheResponse]:
        wire_keys: List[Union[bytes, str]] = [
            self._key_encoder_fn(key) if self._key_encoder_fn else key.key
            for key in keys
        ]
        times and times.lap_serialize()
        try:
            conn = pool.pop_connection()
            times and times.lap_pool()
            error = False
            try:
                responses = conn.meta_multiget(wire_keys, request_flags=flags)
            except Exception as e:
                error = True
                raise MemcacheServerError(pool.server, "Memcache error") from e
            finally:
                pool.release_connection(conn, error=error)
                times and times.lap_network()
        except MemcacheServerError:
            if track_write_failures and self._is_a_write_failure(
                MetaCommand.META_GET, flags
            ):
                for key in keys:
                    self.on_write_failure(key)
            raise_on_server_error = (
                raise_on_server_error
                if raise_on_server_error is not None
                else self._raise_on_server_error
            )
            if raise_on_server_error:
                raise
            return {key: MISS_DUE_TO_ERROR for key in keys}

        processed = {
            key: self._process_response(response)
            for key, response in zip(keys, responses, strict=True)
        }
        times and times.lap_deserialize()
        return processed

    def exec_multi_on_pool(
        self,
        pool: ConnectionPool,
        command: MetaCommand,
        key_values: List[Tuple[Key, MaybeValue]],
        flags: Optional[RequestFlags],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
    ) -> Dict[Key, MemcacheResponse]:
        callback = self._stats_callback
        weight = None if callback is None else sample_weight(self._stats_sampler)
        if weight is None:
            return self._exec_multi_on_pool(
                pool,
                command,
                key_values,
                flags,
                track_write_failures,
                raise_on_server_error,
            )
        times = _OperationTimes()
        responses: Optional[Dict[Key, MemcacheResponse]] = None
        error: Optional[Exception] = None
        try:
            responses = self._exec_multi_on_pool(
                pool,
                command,
                key_values,
                flags,
                track_write_failures,
                raise_on_server_error,
                times,
            )
            return responses
        except Exception as e:
            error = e
            raise
        finally:
            size = 0
            for response in responses.values() if responses else ():
                if isinstance(response, Value):
                    size += response.size
            stats = CacheStats(
                command=command,
                keys=[key for key, _ in key_values],
                size=size,
                server=pool.server,
                flags=flags,
                responses=responses,
                start_time_ns=times.start_ns,
                duration_ns=time.time_ns() - times.start_ns,
                error=error,
                weight=weight,
                serialize_duration_ns=times.serialize_ns,
                pool_duration_ns=times.pool_ns,
                network_duration_ns=times.network_ns,
                deserialize_duration_ns=times.deserialize_ns,
            )
            try:
                # There is a weight, so there is a callback
                callback(stats)  # type: ignore[misc]
            except Exception:
                _log.warning("Error reporting cache stats", exc_info=True)

    def _exec_multi_on_pool(
        self,
        pool: ConnectionPool,
        command: MetaCommand,
        key_values: List[Tuple[Key, MaybeValue]],
        flags: Optional[RequestFlags],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
        times: Optional[_OperationTimes] = None,
    ) -> Dict[Key, MemcacheResponse]:
        if command == MetaCommand.META_GET:
            return self._exec_multi_get_on_pool(
                pool,
                [key for key, _ in key_values],
                flags,
                track_write_failures,
                raise_on_server_error,
                times,
            )
        return self._exec_multi_write_on_pool(
            pool,
            command,
            key_values,
            flags,
            track_write_failures,
            raise_on_server_error,
            times,
        )

    def _exec_multi_write_on_pool(
        self,
        pool: ConnectionPool,
        command: WriteMetaCommand,
        key_values: List[Tuple[Key, MaybeValue]],
        flags: Optional[RequestFlags],
        track_write_failures: bool,
        raise_on_server_error: Optional[bool] = None,
        times: Optional[_OperationTimes] = None,
    ) -> Dict[Key, MemcacheResponse]:
        commands: List[Tuple[Key, Optional[bytes], Optional[RequestFlags]]] = []
        for key, value in key_values:
            if value is None:
                commands.append((key, None, flags))
            else:
                cmd_value, cmd_flags = self._prepare_serialized_value_and_flags(
                    key, value, flags
                )
                commands.append((key, cmd_value, cmd_flags))
        times and times.lap_serialize()

        results: Dict[Key, MemcacheResponse] = {}
        try:
            conn = pool.pop_connection()
            times and times.lap_pool()
            error = False
            try:
                for key, data, key_flags in commands:
                    self._conn_send_cmd(
                        conn,
                        command=command,
                        key=key,
                        value=data,
                        flags=key_flags,
                    )
                # With no_reply there is nothing to read back
                no_reply = flags is not None and flags.no_reply
                for key, _, _ in commands:
                    results[key] = (
                        Success(flags=ResponseFlags())
                        if no_reply
                        else conn.get_response()
                    )
            except Exception as e:
                error = True
                raise MemcacheServerError(pool.server, "Memcache error") from e
            finally:
                pool.release_connection(conn, error=error)
                times and times.lap_network()
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
            failure_result = NOT_STORED_DUE_TO_ERROR
            for key, _ in key_values:
                if key not in results:
                    results[key] = failure_result

        processed = {
            key: self._process_response(response) for key, response in results.items()
        }
        times and times.lap_deserialize()
        return processed

    def _conn_send_cmd(
        self,
        conn: MemcacheSocket,
        command: MetaCommand,
        key: Key,
        value: Optional[bytes] = None,
        flags: Optional[RequestFlags] = None,
    ) -> None:
        """
        Send a command on a connection (for pipelining).
        The send_meta_* methods handle no_reply/noop internally.
        """
        wire_key: Union[str, bytes] = (
            self._key_encoder_fn(key) if self._key_encoder_fn else key.key
        )
        if command == MetaCommand.META_GET:
            conn.send_meta_get(wire_key, flags)
        elif command == MetaCommand.META_SET:
            assert value is not None
            conn.send_meta_set(wire_key, value, flags)
        elif command == MetaCommand.META_DELETE:
            conn.send_meta_delete(wire_key, flags)
        elif command == MetaCommand.META_ARITHMETIC:
            conn.send_meta_arithmetic(wire_key, flags)
        else:
            raise ValueError(f"Unknown command: {command}")
