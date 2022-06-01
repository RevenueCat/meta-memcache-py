import base64
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Callable, DefaultDict, Dict, Final, List, Optional, Set, Tuple

from meta_memcache.base.base_serializer import BaseSerializer
from meta_memcache.base.connection_pool import ConnectionPool
from meta_memcache.base.memcache_socket import MemcacheSocket
from meta_memcache.errors import MemcacheError, MemcacheServerError
from meta_memcache.events.write_failure_event import WriteFailureEvent
from meta_memcache.protocol import (
    ENDL,
    Conflict,
    Flag,
    IntFlag,
    Key,
    MemcacheResponse,
    MetaCommand,
    Miss,
    NotStored,
    ReadResponse,
    ServerVersion,
    Success,
    TokenFlag,
    Value,
    WriteResponse,
    encode_size,
)
from meta_memcache.settings import MAX_KEY_SIZE

_log: logging.Logger = logging.getLogger(__name__)


class BaseCachePool(ABC):
    def __init__(
        self,
        serializer: BaseSerializer,
        binary_key_encoding_fn: Callable[[Key], bytes],
    ) -> None:
        self._serializer = serializer
        self._binary_key_encoding_fn = binary_key_encoding_fn
        self.on_write_failure: Final = WriteFailureEvent()

    @abstractmethod
    def _get_pool(self, key: Key) -> ConnectionPool:
        ...

    def _encode_key(self, key: Key) -> Tuple[bytes, bool]:
        if key.is_unicode or len(key.key) > MAX_KEY_SIZE:
            return base64.b64encode(self._binary_key_encoding_fn(key)), True
        elif " " in key.key:
            raise ValueError(f"Invalid key {key}")
        else:
            return key.key.encode("ascii"), False

    def _build_cmd(
        self,
        command: MetaCommand,
        key: Key,
        size: Optional[int] = None,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
        version: ServerVersion = ServerVersion.STABLE,
    ) -> bytes:
        encoded_key, is_binary = self._encode_key(key)
        cmd = [command.value, encoded_key]
        if size is not None:
            cmd.append(encode_size(size, version=version))
        cmd_flags = []
        if is_binary:
            cmd_flags.append(Flag.BINARY.value)
        if flags:
            cmd_flags.extend(flag.value for flag in flags)
        if int_flags:
            for int_flag, int_value in int_flags.items():
                cmd_flags.append(int_flag.value + str(int_value).encode("ascii"))
        if token_flags:
            for token_flag, bytes_value in token_flags.items():
                cmd_flags.append(token_flag.value + bytes_value)
        cmd.extend(cmd_flags)
        return b" ".join(cmd) + ENDL

    def _exec(
        self,
        command: MetaCommand,
        key: Key,
        value: Optional[bytes] = None,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> MemcacheResponse:
        """
        Gets a connection for the key and executes the command

        You can override to implement retries, having
        a fallback pool, etc.
        """
        try:
            with self._get_pool(key).get_connection() as conn:
                self._conn_send_cmd(
                    conn,
                    command=command,
                    key=key,
                    value=value,
                    flags=flags,
                    int_flags=int_flags,
                    token_flags=token_flags,
                )
                return self._conn_recv_response(conn, flags=flags)
        except MemcacheServerError:
            if command in (MetaCommand.META_DELETE, MetaCommand.META_SET):
                self.on_write_failure(key)
            raise

    def _exec_multi(
        self,
        command: MetaCommand,
        keys: List[Key],
        values: Optional[List[bytes]] = None,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Dict[Key, MemcacheResponse]:
        """
        Groups keys by destination, gets a connection and executes the commands
        """
        if values and len(values) != len(keys):
            raise ValueError("Values needs to match the number of keys")
        key_value_map = {k: v for k, v in zip(keys, values or [])}
        pool_key_map: DefaultDict[ConnectionPool, List[Key]] = defaultdict(list)
        for key in keys:
            pool_key_map[self._get_pool(key)].append(key)

        results: Dict[Key, MemcacheResponse] = {}
        try:
            for pool, pool_keys in pool_key_map.items():
                with pool.get_connection() as conn:
                    for key in pool_keys:
                        self._conn_send_cmd(
                            conn,
                            command=command,
                            key=key,
                            value=key_value_map.get(key),
                            flags=flags,
                            int_flags=int_flags,
                            token_flags=token_flags,
                        )
                    for key in pool_keys:
                        results[key] = self._conn_recv_response(conn, flags=flags)
        except MemcacheServerError:
            if command in (
                MetaCommand.META_DELETE,
                MetaCommand.META_SET,
            ):
                for key in keys:
                    self.on_write_failure(key)
            raise
        return results

    def _conn_send_cmd(
        self,
        conn: MemcacheSocket,
        command: MetaCommand,
        key: Key,
        value: Optional[bytes] = None,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> None:
        """
        Execute command on a connection
        """
        cmd = self._build_cmd(
            command,
            key,
            size=len(value) if value is not None else None,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
            version=conn.get_version(),
        )
        # write meta commands with NOREPLY can potentially return errors
        # they are not fully silent, so we need to add a no-op to the wire.
        with_noop = (
            command != MetaCommand.META_GET
            and flags is not None
            and Flag.NOREPLY in flags
        )

        if value:
            conn.sendall(cmd + value + ENDL, with_noop=with_noop)
        else:
            conn.sendall(cmd, with_noop=with_noop)

    def _conn_recv_response(
        self,
        conn: MemcacheSocket,
        flags: Optional[Set[Flag]] = None,
    ) -> MemcacheResponse:
        """
        Read response on a connection
        """
        if flags and Flag.NOREPLY in flags:
            return Success(flags=set([Flag.NOREPLY]))
        result = conn.get_response()
        if isinstance(result, Value):
            data = conn.get_value(result.size)
            if result.size > 0:
                encoding_id = result.int_flags.get(IntFlag.CLIENT_FLAG, 0)
                try:
                    result.value = self._serializer.unserialize(data, encoding_id)
                except Exception:
                    _log.exception("Error unserializing value")
                    result = Miss()

        return result

    def meta_multiget(
        self,
        keys: List[Key],
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Dict[Key, ReadResponse]:

        results: Dict[Key, ReadResponse] = {}
        for key, result in self._exec_multi(
            command=MetaCommand.META_GET,
            keys=keys,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        ).items():
            if not isinstance(result, (Miss, Value, Success)):
                raise MemcacheError(
                    f"Unexpected response for Meta Get command: {result}"
                )
            results[key] = result
        return results

    def meta_get(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> ReadResponse:
        result = self._exec(
            command=MetaCommand.META_GET,
            key=key,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )
        if not isinstance(result, (Miss, Value, Success)):
            raise MemcacheError(f"Unexpected response for Meta Get command: {result}")
        return result

    def meta_set(
        self,
        key: Key,
        value: Any,
        ttl: int,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        encoded_value = self._serializer.serialize(value)
        if int_flags is None:
            int_flags = {}
        int_flags[IntFlag.SET_CLIENT_FLAG] = encoded_value.encoding_id

        result = self._exec(
            command=MetaCommand.META_SET,
            key=key,
            value=encoded_value.data,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )
        if not isinstance(result, (Success, NotStored, Conflict, Miss)):
            raise MemcacheError(f"Unexpected response for Meta Set command: {result}")
        return result

    def meta_delete(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        result = self._exec(
            command=MetaCommand.META_DELETE,
            key=key,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )
        if not isinstance(result, (Success, NotStored, Conflict, Miss)):
            raise MemcacheError(
                f"Unexpected response for Meta Delete command: {result}"
            )
        return result

    def meta_arithmetic(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        result = self._exec(
            command=MetaCommand.META_ARITHMETIC,
            key=key,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )
        if not isinstance(result, (Success, NotStored, Conflict, Miss, Value)):
            raise MemcacheError(
                f"Unexpected response for Meta Delete command: {result}"
            )
        return result
