import base64
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, Set, Tuple, Union

from meta_memcache.base.base_serializer import BaseSerializer
from meta_memcache.base.base_write_failure_tracker import BaseWriteFailureTracker
from meta_memcache.base.connection_pool import ConnectionPool
from meta_memcache.base.memcache_socket import MemcacheSocket
from meta_memcache.errors import MemcacheError, MemcacheServerError
from meta_memcache.protocol import (
    ENDL,
    Conflict,
    Flag,
    IntFlag,
    Key,
    MetaCommand,
    Miss,
    NotStored,
    Success,
    TokenFlag,
    Value,
)
from meta_memcache.settings import MAX_KEY_SIZE


class BaseCachePool(ABC):
    def __init__(
        self,
        serializer: BaseSerializer,
        binary_key_encoding_fn: Callable[[Key], bytes],
        write_failure_tracker: Optional[BaseWriteFailureTracker] = None,
    ) -> None:
        self._serializer = serializer
        self._binary_key_encoding_fn = binary_key_encoding_fn
        self._write_failure_tracker = write_failure_tracker

    @abstractmethod
    def _get_pool(self, key: Key) -> ConnectionPool:
        pass

    def _encode_key(self, key: Key) -> Tuple[bytes, bool]:
        if key.is_unicode or len(key.key) > MAX_KEY_SIZE:
            return base64.b64encode(self._binary_key_encoding_fn(key)), True
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
    ) -> bytes:
        encoded_key, is_binary = self._encode_key(key)
        cmd = [command.value, encoded_key]
        if size is not None:
            cmd.append(str(size).encode("ascii"))
        cmd_flags = []
        if is_binary:
            cmd_flags.append(Flag.BINARY.value)
        if flags:
            cmd_flags.extend(sorted(flag.value for flag in flags))
        if int_flags:
            for flag, int_value in int_flags.items():
                cmd_flags.append(flag.value + str(int_value).encode("ascii"))
        if token_flags:
            for flag, bytes_value in token_flags.items():
                cmd_flags.append(flag.value + bytes_value)
        cmd.extend(sorted(cmd_flags))
        return b" ".join(cmd) + ENDL

    def _exec(
        self,
        command: MetaCommand,
        key: Key,
        value: Optional[bytes] = None,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Union[Miss, Value, Success, NotStored, Conflict]:
        """
        Gets a connection for key key and executes the command

        You can override to implement retries, having
        a fallback pool, etc.
        """
        try:
            with self._get_pool(key).get_connection() as c:
                return self._exec_on_connection(
                    c,
                    command=command,
                    key=key,
                    value=value,
                    flags=flags,
                    int_flags=int_flags,
                    token_flags=token_flags,
                )
        except MemcacheServerError:
            if self._write_failure_tracker:
                if command in (MetaCommand.MD, MetaCommand.MS):
                    self._write_failure_tracker.add_key(key)
            raise

    def _exec_on_connection(
        self,
        c: MemcacheSocket,
        command: MetaCommand,
        key: Key,
        value: Optional[bytes] = None,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Union[Miss, Value, Success, NotStored, Conflict]:
        """
        Execute command on a connection and read response back
        """
        cmd = self._build_cmd(
            command,
            key,
            size=len(value) if value is not None else None,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )
        if value:
            c.sendall(cmd + value + ENDL)
        else:
            c.sendall(cmd)
        if flags and Flag.NOREPLY in flags:
            return Success(flags=set([Flag.NOREPLY]))
        result = c.get_response()
        if isinstance(result, Value):
            # TODO: Confirm this works for empty values!
            data = c.get_value(result.size)
            if result.size > 0:
                encoding_id = result.int_flags.get(IntFlag.CLIENT_FLAG, 0)
                result.value = self._serializer.unserialize(data, encoding_id)
        return result

    def mg(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Union[Miss, Value, Success]:
        result = self._exec(
            command=MetaCommand.MG,
            key=key,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )
        if not isinstance(result, (Miss, Value, Success)):
            raise MemcacheError(
                f"Unexpected response for Meta Delete command: {result}"
            )
        return result

    def ms(
        self,
        key: Key,
        value: Any,  # pyre-ignore[2]
        ttl: int,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Union[Success, NotStored, Conflict, Miss]:
        encoded_value = self._serializer.serialize(value)
        if int_flags is None:
            int_flags = {}
        int_flags[IntFlag.SET_CLIENT_FLAG] = encoded_value.encoding_id

        result = self._exec(
            command=MetaCommand.MS,
            key=key,
            value=encoded_value.data,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )
        if not isinstance(result, (Success, NotStored, Conflict, Miss)):
            raise MemcacheError(
                f"Unexpected response for Meta Delete command: {result}"
            )
        return result

    def md(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Union[Success, NotStored, Conflict, Miss]:
        result = self._exec(
            command=MetaCommand.MD,
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

    # TODO: Implement Meta Arithmetic
