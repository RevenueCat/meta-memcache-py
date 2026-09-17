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
    Value,
    Success,
    Miss,
    NotStored,
    Conflict,
    SERVER_VERSION_STABLE,
    SERVER_VERSION_AWS_1_6_6,
)

ENDL = b"\r\n"
NOOP: bytes = b"mn" + ENDL
ENDL_LEN = 2
SPACE: int = ord(" ")


@dataclass
class Key:
    __slots__ = ("key", "routing_key", "domain", "disable_compression")
    key: str
    routing_key: Optional[str]
    domain: Optional[str]
    disable_compression: bool

    def __init__(
        self,
        key: str,
        routing_key: Optional[str] = None,
        domain: Optional[str] = None,
        disabled_compression: bool = False,
    ) -> None:
        self.key = key
        self.routing_key = routing_key
        self.domain = domain
        self.disable_compression = disabled_compression

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


MemcacheResponse = Union[Value, Success, Miss, NotStored, Conflict]

# Responses the server never actually sent: the request failed and the pool is
# configured not to raise, so the failure is reported as the closest negative
# response, a miss for reads and a not-stored for writes.
#
# They are a plain Miss/NotStored for every isinstance() check, so nothing that
# handles those needs to know about them, but code that cares about the
# difference between "the server said no" and "we could not ask" can tell them
# apart with is_error_response(). Miss and NotStored come from the rust
# extension and cannot be subclassed, hence the marker instances.
#
# The markers are recognized by identity, with a `is` check.
MISS_DUE_TO_ERROR: Miss = Miss()
NOT_STORED_DUE_TO_ERROR: NotStored = NotStored()


def is_error_response(response: MemcacheResponse) -> bool:
    """Whether a response is really a server failure."""
    return response is MISS_DUE_TO_ERROR or response is NOT_STORED_DUE_TO_ERROR


@dataclass
class ValueContainer:
    __slots__ = ("value",)
    value: Any


MaybeValue = Optional[ValueContainer]
MaybeValues = Optional[List[ValueContainer]]


ReadResponse = Union[Miss, Value, Success]
WriteResponse = Union[Success, NotStored, Conflict, Miss]
ArithmeticResponse = Union[Success, NotStored, Conflict, Miss, Value]


Blob = Union[bytes, bytearray, memoryview]


class ServerVersion(IntEnum):
    """
    If more versions with breaking changes are
    added, bump stable to the next int. Code
    will be able to use > / < / = to code
    the behavior of the different versions.
    """

    AWS_1_6_6 = SERVER_VERSION_AWS_1_6_6
    STABLE = SERVER_VERSION_STABLE


def get_store_success_response_header(version: ServerVersion) -> bytes:
    if version == ServerVersion.AWS_1_6_6:
        return b"OK"
    return b"HD"
