import pickle  # noqa: S403
import zlib
from typing import Any, List, NamedTuple, Tuple

from meta_memcache.base.base_serializer import BaseSerializer, EncodedValue
from meta_memcache.compression.zstd_manager import BaseZstdManager
from meta_memcache.protocol import Blob, Key
import zstandard as zstd


class MixedSerializer(BaseSerializer):
    STR = 0
    PICKLE = 1
    INT = 2
    LONG = 4
    ZLIB_COMPRESSED = 8
    BINARY = 16

    COMPRESSION_THRESHOLD = 128

    def __init__(self, pickle_protocol: int = 0) -> None:
        self._pickle_protocol = pickle_protocol

    def serialize(
        self,
        key: Key,
        value: Any,
    ) -> EncodedValue:
        if isinstance(value, bytes):
            data = value
            encoding_id = self.BINARY
        elif isinstance(value, int) and not isinstance(value, bool):
            data = str(value).encode("ascii")
            encoding_id = self.INT
        elif isinstance(value, str):
            data = str(value).encode()
            encoding_id = self.STR
        else:
            data = pickle.dumps(value, protocol=self._pickle_protocol)
            encoding_id = self.PICKLE

        if len(data) > self.COMPRESSION_THRESHOLD:
            encoding_id |= self.ZLIB_COMPRESSED
            data = zlib.compress(data)
        return EncodedValue(data=data, encoding_id=encoding_id)

    def unserialize(self, data: Blob, encoding_id: int) -> Any:
        if encoding_id & self.ZLIB_COMPRESSED:
            data = zlib.decompress(data)
            encoding_id ^= self.ZLIB_COMPRESSED

        if encoding_id == self.STR:
            return bytes(data).decode()
        elif encoding_id in (self.INT, self.LONG):
            return int(data)
        elif encoding_id == self.BINARY:
            return bytes(data)
        else:
            return pickle.loads(data)  # noqa: S301


class DictionaryMapping(NamedTuple):
    dictionary: bytes
    active_domains: List[str]


class ZstdSerializer(BaseSerializer):
    STR = 0
    PICKLE = 1
    INT = 2
    LONG = 4
    ZLIB_COMPRESSED = 8
    BINARY = 16
    ZSTD_COMPRESSED = 32

    DEFAULT_PICKLE_PROTOCOL = 5
    DEFAULT_COMPRESSION_LEVEL = 9
    DEFAULT_COMPRESSION_THRESHOLD = 128
    DEFAULT_DICT_COMPRESSION_THRESHOLD = 64
    DEFAULT_ZSTD_FORMAT = zstd.FORMAT_ZSTD1_MAGICLESS

    _zstd_manager: BaseZstdManager
    _pickle_protocol: int
    _default_compression_threshold: int
    _dict_compression_threshold: int
    _default_zstd: bool

    def __init__(
        self,
        zstd_manager: BaseZstdManager,
        pickle_protocol: int = DEFAULT_PICKLE_PROTOCOL,
        compression_threshold: int = DEFAULT_COMPRESSION_THRESHOLD,
        dict_compression_threshold: int = DEFAULT_DICT_COMPRESSION_THRESHOLD,
        default_zstd: bool = True,
    ) -> None:
        self._zstd_manager = zstd_manager
        self._pickle_protocol = pickle_protocol
        self._default_compression_threshold = compression_threshold
        self._dict_compression_threshold = dict_compression_threshold
        self._default_zstd = default_zstd

    def _compress(self, key: Key, data: bytes) -> Tuple[bytes, int]:
        if not self._default_zstd:
            return zlib.compress(data), self.ZLIB_COMPRESSED

        compressed = self._zstd_manager.compress(data, key.domain)
        return compressed, self.ZSTD_COMPRESSED

    def _should_compress(self, key: Key, data: bytes) -> bool:
        data_len = len(data)
        if data_len >= self._default_compression_threshold:
            return True
        elif data_len >= self._dict_compression_threshold:
            return bool(self._zstd_manager.select_dict_id(key.domain))
        return False

    def serialize(
        self,
        key: Key,
        value: Any,
    ) -> EncodedValue:
        if isinstance(value, bytes):
            data = value
            encoding_id = self.BINARY
        elif isinstance(value, int) and not isinstance(value, bool):
            data = str(value).encode("ascii")
            encoding_id = self.INT
        elif isinstance(value, str):
            data = str(value).encode()
            encoding_id = self.STR
        else:
            data = pickle.dumps(value, protocol=self._pickle_protocol)
            encoding_id = self.PICKLE

        if not key.disable_compression and self._should_compress(key, data):
            data, compression_flag = self._compress(key, data)
            encoding_id |= compression_flag
        return EncodedValue(data=data, encoding_id=encoding_id)

    def unserialize(self, data: Blob, encoding_id: int) -> Any:
        if encoding_id & self.ZLIB_COMPRESSED:
            data = zlib.decompress(data)
            encoding_id ^= self.ZLIB_COMPRESSED
        elif encoding_id & self.ZSTD_COMPRESSED:
            data = self._zstd_manager.decompress(data)
            encoding_id ^= self.ZSTD_COMPRESSED

        if encoding_id == self.STR:
            return bytes(data).decode()
        elif encoding_id in (self.INT, self.LONG):
            return int(data)
        elif encoding_id == self.BINARY:
            return bytes(data)
        else:
            return pickle.loads(data)  # noqa: S301
