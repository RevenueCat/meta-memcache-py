from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, Dict, List, Optional, Union

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
TOKEN_FLAG_OPAQUE = ord("O")
INT_FLAG_CAS_TOKEN = ord("c")
INT_FLAG_FETCHED = ord("h")
INT_FLAG_LAST_ACCESS = ord("l")
INT_FLAG_TTL = ord("t")
INT_FLAG_CLIENT_FLAG = ord("f")
INT_FLAG_SIZE = ord("s")
FLAG_WIN = ord("W")
FLAG_LOST = ord("Z")
FLAG_STALE = ord("X")


# @dataclass(slots=True, init=False)
@dataclass
class Success(MemcacheResponse):
    __slots__ = (
        "cas_token",
        "fetched",
        "last_access",
        "ttl",
        "client_flag",
        "win",
        "stale",
        "real_size",
        "opaque",
    )
    cas_token: Optional[int]
    fetched: Optional[int]
    last_access: Optional[int]
    ttl: Optional[int]
    client_flag: Optional[int]
    win: Optional[bool]
    stale: bool
    real_size: Optional[int]
    opaque: Optional[bytes]

    def __init__(
        self,
        *,
        cas_token: Optional[int] = None,
        fetched: Optional[int] = None,
        last_access: Optional[int] = None,
        ttl: Optional[int] = None,
        client_flag: Optional[int] = None,
        win: Optional[bool] = None,
        stale: bool = False,
        real_size: Optional[int] = None,
        opaque: Optional[bytes] = None,
    ) -> None:
        self.cas_token = cas_token
        self.fetched = fetched
        self.last_access = last_access
        self.ttl = ttl
        self.client_flag = client_flag
        self.win = win
        self.stale = stale
        self.real_size = real_size
        self.opaque = opaque

    @classmethod
    def from_header(cls, header: "Blob") -> "Success":
        result = cls()
        result._set_flags(header)
        return result

    def _set_flags(self, header: bytes, pos: int = 3) -> None:  # noqa: C901
        header_size = len(header)
        while pos < header_size:
            flag = header[pos]
            pos += 1
            if flag == SPACE:
                continue
            end = pos
            while end < header_size:
                if header[end] == SPACE:
                    break
                end += 1

            if flag == INT_FLAG_CAS_TOKEN:
                self.cas_token = int(header[pos:end])
            elif flag == INT_FLAG_FETCHED:
                self.fetched = int(header[pos:end])
            elif flag == INT_FLAG_LAST_ACCESS:
                self.last_access = int(header[pos:end])
            elif flag == INT_FLAG_TTL:
                self.ttl = int(header[pos:end])
            elif flag == INT_FLAG_CLIENT_FLAG:
                self.client_flag = int(header[pos:end])
            elif flag == FLAG_WIN:
                self.win = True
            elif flag == FLAG_LOST:
                self.win = False
            elif flag == FLAG_STALE:
                self.stale = True
            elif flag == INT_FLAG_SIZE:
                self.real_size = int(header[pos:end])
            elif flag == TOKEN_FLAG_OPAQUE:
                self.opaque = header[pos:end]
            pos = end + 1


# @dataclass(slots=True, init=False)
@dataclass
class Value(Success):
    __slots__ = (
        "cas_token",
        "fetched",
        "last_access",
        "ttl",
        "client_flag",
        "win",
        "stale",
        "real_size",
        "opaque",
        "size",
        "value",
    )
    size: int
    value: Optional[Any]

    def __init__(
        self,
        *,
        size: int,
        value: Optional[Any] = None,
        cas_token: Optional[int] = None,
        fetched: Optional[int] = None,
        last_access: Optional[int] = None,
        ttl: Optional[int] = None,
        client_flag: Optional[int] = None,
        win: Optional[bool] = None,
        stale: bool = False,
        real_size: Optional[int] = None,
        opaque: Optional[bytes] = None,
    ) -> None:
        self.size = size
        self.value = value
        self.cas_token = cas_token
        self.fetched = fetched
        self.last_access = last_access
        self.ttl = ttl
        self.client_flag = client_flag
        self.win = win
        self.stale = stale
        self.real_size = real_size
        self.opaque = opaque

    @classmethod
    def from_header(cls, header: "Blob") -> "Value":
        header_size = len(header)
        if header_size < 4 or header[2] != SPACE:
            raise ValueError(f"Invalid header {header!r}")
        end = 4
        while end < header_size:
            if header[end] == SPACE:
                break
            end += 1
        size = int(header[3:end])
        result = cls(size=size)
        result._set_flags(header, pos=end + 1)
        return result


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
