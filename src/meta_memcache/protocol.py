from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, List, Optional, Union

from meta_memcache_socket import (  # noqa: F401
    ResponseFlags,
    RequestFlags as RequestFlags,
    SET_MODE_ADD,
    SET_MODE_APPEND,
    SET_MODE_PREPEND,
    SET_MODE_REPLACE,
    SET_MODE_SET,
    MA_MODE_INC as MA_MODE_INC,
    MA_MODE_DEC as MA_MODE_DEC,
)

ENDL = b"\r\n"
NOOP: bytes = b"mn" + ENDL
ENDL_LEN = 2
SPACE: int = ord(" ")


@dataclass
class Key:
    __slots__ = ("key", "routing_key", "is_unicode")
    key: str
    routing_key: Optional[str]
    is_unicode: bool

    def __init__(
        self,
        key: str,
        routing_key: Optional[str] = None,
        is_unicode: bool = False,
    ) -> None:
        self.key = key
        self.routing_key = routing_key
        self.is_unicode = is_unicode

    def __hash__(self) -> int:
        return hash((self.key, self.routing_key))


class MetaCommand(Enum):
    META_GET = b"mg"  # Meta Get
    META_SET = b"ms"  # Meta Set
    META_DELETE = b"md"  # Meta Delete
    META_ARITHMETIC = b"ma"  # Meta Arithmetic


class SetMode(Enum):
    SET = SET_MODE_SET  # Default
    ADD = SET_MODE_ADD  # Add if item does NOT EXIST, else LRU bump and return NS
    APPEND = SET_MODE_APPEND  # If item exists, append the new value to its data.
    PREPEND = SET_MODE_PREPEND  # If item exists, prepend the new value to its data.
    REPLACE = SET_MODE_REPLACE  # Set only if item already exists.


@dataclass
class MemcacheResponse:
    __slots__ = ()


@dataclass
class Miss(MemcacheResponse):
    __slots__ = ()

    pass


@dataclass
class Success(MemcacheResponse):
    __slots__ = ("flags",)
    flags: ResponseFlags


@dataclass
class Value(Success):
    __slots__ = ("flags", "size", "value")
    size: int
    value: Optional[Any]


@dataclass
class ValueContainer:
    __slots__ = ("value",)
    value: Any


MaybeValue = Optional[ValueContainer]
MaybeValues = Optional[List[ValueContainer]]


@dataclass
class NotStored(MemcacheResponse):
    __slots__ = ()


@dataclass
class Conflict(MemcacheResponse):
    __slots__ = ()


ReadResponse = Union[Miss, Value, Success]
WriteResponse = Union[Success, NotStored, Conflict, Miss]


Blob = Union[bytes, bytearray, memoryview]


class ServerVersion(IntEnum):
    """
    If more versions with breaking changes are
    added, bump stable to the next int. Code
    will be able to use > / < / = to code
    the behavior of the different versions.
    """

    AWS_1_6_6 = 1
    STABLE = 2


def get_store_success_response_header(version: ServerVersion) -> bytes:
    if version == ServerVersion.AWS_1_6_6:
        return b"OK"
    return b"HD"
