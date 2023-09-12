from typing import Any, Dict, List, Optional, Set

from meta_memcache.errors import MemcacheError
from meta_memcache.interfaces.router import HasRouter
from meta_memcache.protocol import (
    Conflict,
    Flag,
    IntFlag,
    Key,
    MetaCommand,
    Miss,
    NotStored,
    ReadResponse,
    Success,
    TokenFlag,
    Value,
    ValueContainer,
    WriteResponse,
)


class MetaCommandsMixin:
    def meta_multiget(
        self: HasRouter,
        keys: List[Key],
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Dict[Key, ReadResponse]:
        results: Dict[Key, ReadResponse] = {}
        for key, result in self.router.exec_multi(
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
        self: HasRouter,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> ReadResponse:
        result = self.router.exec(
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
        self: HasRouter,
        key: Key,
        value: Any,
        ttl: int,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        result = self.router.exec(
            command=MetaCommand.META_SET,
            key=key,
            value=ValueContainer(value),
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )
        if not isinstance(result, (Success, NotStored, Conflict, Miss)):
            raise MemcacheError(f"Unexpected response for Meta Set command: {result}")
        return result

    def meta_delete(
        self: HasRouter,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        result = self.router.exec(
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
        self: HasRouter,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        result = self.router.exec(
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
