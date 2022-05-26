from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, Dict, NamedTuple, Optional, Set, Union

ENDL = b"\r\n"
NOOP: bytes = b"mn" + ENDL
ENDL_LEN = 2
SPACE: int = ord(" ")


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
    RETURNED_CAS_TOKEN = b"c"
    CAS_TOKEN = b"C"


class TokenFlag(Enum):
    OPAQUE = b"O"
    KEY = b"k"
    # 'M' (mode switch):
    # * Meta Arithmetic:
    #  - I or +: increment
    #  - D or -: decrement
    # * Meta Set: See SetMode Enum in base/cache_pool.py
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
    value: Optional[Any]

    def __init__(
        self,
        size: int,
        value: Optional[Any] = None,
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
