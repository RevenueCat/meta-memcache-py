from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, Dict, List, Optional, Union

from meta_memcache_socket import ResponseFlags

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
    SET = b"S"  # Default
    ADD = b"E"  # Add if item does NOT EXIST, else LRU bump and return NS
    APPEND = b"A"  # If item exists, append the new value to its data.
    PREPEND = b"P"  # If item exists, prepend the new value to its data.
    REPLACE = b"R"  # Set only if item already exists.


class Flag(Enum):
    BINARY = b"b"
    NOREPLY = b"q"
    RETURN_CLIENT_FLAG = b"f"
    RETURN_CAS_TOKEN = b"c"
    RETURN_VALUE = b"v"
    RETURN_TTL = b"t"
    RETURN_SIZE = b"s"
    RETURN_LAST_ACCESS = b"l"
    RETURN_FETCHED = b"h"
    RETURN_KEY = b"k"
    NO_UPDATE_LRU = b"u"
    MARK_STALE = b"I"


class IntFlag(Enum):
    CACHE_TTL = b"T"
    RECACHE_TTL = b"R"
    MISS_LEASE_TTL = b"N"
    SET_CLIENT_FLAG = b"F"
    MA_INITIAL_VALUE = b"J"
    MA_DELTA_VALUE = b"D"
    CAS_TOKEN = b"C"


class TokenFlag(Enum):
    OPAQUE = b"O"
    # 'M' (mode switch):
    # * Meta Arithmetic:
    #  - I or +: increment
    #  - D or -: decrement
    # * Meta Set: See SetMode Enum above
    #  - E: "add" command. LRU bump and return NS if item exists. Else add.
    #  - A: "append" command. If item exists, append the new value to its data.
    #  - P: "prepend" command. If item exists, prepend the new value to its data.
    #  - R: "replace" command. Set only if item already exists.
    #  - S: "set" command. The default mode, added for completeness.
    MODE = b"M"


# Store maps of byte values (int) to enum value
flag_values: Dict[int, Flag] = {f.value[0]: f for f in Flag}
int_flags_values: Dict[int, IntFlag] = {f.value[0]: f for f in IntFlag}
token_flags_values: Dict[int, TokenFlag] = {f.value[0]: f for f in TokenFlag}


@dataclass
class MemcacheResponse:
    __slots__ = ()


@dataclass
class Miss(MemcacheResponse):
    __slots__ = ()

    pass


# Response flags
EMPTY_RESPONSE_FLAGS = ResponseFlags()


@dataclass
class Success(MemcacheResponse):
    __slots__ = ("flags",)
    flags: ResponseFlags

    @classmethod
    def default(cls) -> "Success":
        return cls(flags=ResponseFlags())


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


def encode_size(size: int, version: ServerVersion) -> bytes:
    if version == ServerVersion.AWS_1_6_6:
        return b"S" + str(size).encode("ascii")
    else:
        return str(size).encode("ascii")
