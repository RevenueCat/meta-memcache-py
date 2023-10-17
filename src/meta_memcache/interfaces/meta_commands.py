from typing import Any, Dict, List, Optional, Protocol, Set

from meta_memcache.interfaces.router import FailureHandling, DEFAULT_FAILURE_HANDLING
from meta_memcache.protocol import (
    Flag,
    IntFlag,
    Key,
    ReadResponse,
    TokenFlag,
    WriteResponse,
)


class MetaCommandsProtocol(Protocol):
    def meta_multiget(
        self,
        keys: List[Key],
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> Dict[Key, ReadResponse]:
        ...  # pragma: no cover

    def meta_get(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> ReadResponse:
        ...  # pragma: no cover

    def meta_set(
        self,
        key: Key,
        value: Any,
        ttl: int,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        ...  # pragma: no cover

    def meta_delete(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        ...  # pragma: no cover

    def meta_arithmetic(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
        failure_handling: FailureHandling = DEFAULT_FAILURE_HANDLING,
    ) -> WriteResponse:
        ...  # pragma: no cover
