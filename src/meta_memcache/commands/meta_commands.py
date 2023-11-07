from typing import Any, Dict, List, Optional

from meta_memcache.errors import MemcacheError
from meta_memcache.interfaces.router import (
    DEFAULT_FAILURE_HANDLING,
    FailureHandling,
    HasRouter,
)
from meta_memcache.protocol import (
    Conflict,
    Key,
    MetaCommand,
    Miss,
    NotStored,
    ReadResponse,
    RequestFlags,
    Success,
    Value,
    ValueContainer,
    WriteResponse,
)


class MetaCommandsMixin:
    def meta_multiget(
        self: HasRouter,
        keys: List[Key],
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> Dict[Key, ReadResponse]:
        results: Dict[Key, ReadResponse] = {}
        for key, result in self.router.exec_multi(
            command=MetaCommand.META_GET,
            keys=keys,
            flags=flags,
            failure_handling=failure_handling,
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
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> ReadResponse:
        result = self.router.exec(
            command=MetaCommand.META_GET,
            key=key,
            flags=flags,
            failure_handling=failure_handling,
        )
        if not isinstance(result, (Miss, Value, Success)):
            raise MemcacheError(f"Unexpected response for Meta Get command: {result}")
        return result

    def meta_set(
        self: HasRouter,
        key: Key,
        value: Any,
        ttl: int,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        result = self.router.exec(
            command=MetaCommand.META_SET,
            key=key,
            value=ValueContainer(value),
            flags=flags,
            failure_handling=failure_handling,
        )
        if not isinstance(result, (Success, NotStored, Conflict, Miss)):
            raise MemcacheError(f"Unexpected response for Meta Set command: {result}")
        return result

    def meta_delete(
        self: HasRouter,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        result = self.router.exec(
            command=MetaCommand.META_DELETE,
            key=key,
            flags=flags,
            failure_handling=failure_handling,
        )
        if not isinstance(result, (Success, NotStored, Conflict, Miss)):
            raise MemcacheError(
                f"Unexpected response for Meta Delete command: {result}"
            )
        return result

    def meta_arithmetic(
        self: HasRouter,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        result = self.router.exec(
            command=MetaCommand.META_ARITHMETIC,
            key=key,
            flags=flags,
            failure_handling=failure_handling,
        )
        if not isinstance(result, (Success, NotStored, Conflict, Miss, Value)):
            raise MemcacheError(
                f"Unexpected response for Meta Delete command: {result}"
            )
        return result
