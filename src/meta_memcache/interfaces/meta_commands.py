from typing import Any, Dict, List, Optional, Protocol

from meta_memcache.interfaces.router import FailureHandling, DEFAULT_FAILURE_HANDLING
from meta_memcache.protocol import (
    Key,
    ReadResponse,
    WriteResponse,
    RequestFlags,
)


class MetaCommandsProtocol(Protocol):
    def meta_multiget(
        self,
        keys: List[Key],
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> Dict[Key, ReadResponse]:
        ...  # pragma: no cover

    def meta_get(
        self,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> ReadResponse:
        ...  # pragma: no cover

    def meta_set(
        self,
        key: Key,
        value: Any,
        ttl: int,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        ...  # pragma: no cover

    def meta_delete(
        self,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        ...  # pragma: no cover

    def meta_arithmetic(
        self,
        key: Key,
        flags: Optional[RequestFlags] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        ...  # pragma: no cover
