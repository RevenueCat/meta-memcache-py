from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, NamedTuple, Optional, Set, Union

ENDL = b"\r\n"
ENDL_LEN = 2
SPACE: int = ord(" ")
MIN_HEADER_SIZE = 4


class Key(NamedTuple):
    key: str
    routing_key: Optional[str] = None
    is_unicode: bool = False


class MetaCommand(Enum):
    META_GET = b"mg"  # Meta Get
    META_SET = b"ms"  # Meta Set
    META_DELETE = b"md"  # Meta Delete
    META_ARITHMETIC = b"ma"  # Meta Arithmetic


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
    WIN = b"W"
    LOST = b"Z"
    STALE = b"X"
    MARK_STALE = b"I"


class IntFlag(Enum):
    TTL = b"t"
    CACHE_TTL = b"T"
    RECACHE_TTL = b"R"
    MISS_LEASE_TTL = b"N"
    CLIENT_FLAG = b"f"
    SET_CLIENT_FLAG = b"F"
    LAST_READ_AGE = b"l"
    HIT_AFTER_WRITE = b"h"
    MA_INITIAL_VALUE = b"J"
    MA_DELTA_VALUE = b"D"
    CAS_TOKEN = b"c"


class TokenFlag(Enum):
    OPAQUE = b"O"
    KEY = b"k"
    MA_MODE = b"M"  # mode switch. I or + / D or - for incr / decr


# Store maps of byte values (int) to enum value
flag_values: Dict[int, Flag] = {f.value[0]: f for f in Flag}
int_flags_values: Dict[int, IntFlag] = {f.value[0]: f for f in IntFlag}
token_flags_values: Dict[int, TokenFlag] = {f.value[0]: f for f in TokenFlag}


class MemcacheResponse:
    pass


class Miss(MemcacheResponse):
    pass


@dataclass
class Success(MemcacheResponse):
    flags: Set[Flag]
    int_flags: Dict[IntFlag, int]
    token_flags: Dict[TokenFlag, bytes]

    def __init__(
        self,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> None:
        self.flags = flags or set()
        self.int_flags = int_flags or {}
        self.token_flags = token_flags or {}


@dataclass
class Value(Success):
    size: int
    value: Optional[Any]  # pyre-ignore[4]

    def __init__(
        self,
        size: int,
        value: Optional[Any] = None,  # pyre-ignore[2]
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> None:
        super().__init__(flags, int_flags, token_flags)
        self.size = size
        self.value = value


@dataclass
class NotStored(MemcacheResponse):
    pass


@dataclass
class Conflict(MemcacheResponse):
    pass


ReadResponse = Union[Miss, Value, Success]
WriteResponse = Union[Success, NotStored, Conflict, Miss]


Blob = Union[bytes, bytearray, memoryview]
