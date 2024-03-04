import pickle  # noqa: S403
import zlib
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from meta_memcache.base.base_serializer import BaseSerializer, EncodedValue
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

    ZSTD_MAGIC = b"(\xb5/\xfd"
    DEFAULT_PICKLE_PROTOCOL = 5
    DEFAULT_COMPRESSION_LEVEL = 9
    DEFAULT_COMPRESSION_THRESHOLD = 128
    DEFAULT_DICT_COMPRESSION_THRESHOLD = 64

    _pickle_protocol: int
    _compression_level: int
    _default_compression_threshold: int
    _dict_compression_threshold: int
    _zstd_compressors: Dict[int, zstd.ZstdCompressor]
    _zstd_decompressors: Dict[int, zstd.ZstdDecompressor]
    _domain_to_dict_id: Dict[str, int]
    _default_zstd_compressor: Optional[zstd.ZstdCompressor]

    def __init__(
        self,
        pickle_protocol: int = DEFAULT_PICKLE_PROTOCOL,
        compression_level: int = DEFAULT_COMPRESSION_LEVEL,
        compression_threshold: int = DEFAULT_COMPRESSION_THRESHOLD,
        dict_compression_threshold: int = DEFAULT_DICT_COMPRESSION_THRESHOLD,
        dictionary_mappings: Optional[List[DictionaryMapping]] = None,
        default_dictionary: Optional[bytes] = None,
        default_zstd: bool = True,
    ) -> None:
        self._pickle_protocol = pickle_protocol
        self._compression_level = compression_level
        self._default_compression_threshold = (
            compression_threshold
            if not default_dictionary
            else dict_compression_threshold
        )
        self._dict_compression_threshold = dict_compression_threshold
        self._zstd_compressors = {}
        self._zstd_decompressors = {}
        self._domain_to_dict_id = {}

        compression_params = zstd.ZstdCompressionParameters.from_level(
            compression_level,
            format=zstd.FORMAT_ZSTD1_MAGICLESS,
            write_content_size=True,
            write_checksum=False,
            write_dict_id=True,
        )

        if dictionary_mappings:
            for dictionary_mapping in dictionary_mappings:
                dict_id, zstd_dict = self._build_dict(dictionary_mapping.dictionary)
                self._add_dict_decompressor(dict_id, zstd_dict)
                if dictionary_mapping.active_domains:
                    # The dictionary is active for some domains
                    self._add_dict_compressor(dict_id, zstd_dict, compression_params)
                    for domain in dictionary_mapping.active_domains:
                        self._domain_to_dict_id[domain] = dict_id

        if default_dictionary:
            dict_id, zstd_dict = self._build_dict(default_dictionary)
            self._add_dict_decompressor(dict_id, zstd_dict)

            self._default_zstd_compressor = self._add_dict_compressor(
                dict_id, zstd_dict, compression_params
            )
        elif default_zstd:
            self._default_zstd_compressor = zstd.ZstdCompressor(
                compression_params=compression_params
            )
        else:
            self._default_zstd_compressor = None

        self._zstd_decompressors[0] = zstd.ZstdDecompressor()

    def _build_dict(self, dictionary: bytes) -> Tuple[int, zstd.ZstdCompressionDict]:
        zstd_dict = zstd.ZstdCompressionDict(dictionary)
        dict_id = zstd_dict.dict_id()
        return dict_id, zstd_dict

    def _add_dict_decompressor(
        self, dict_id: int, zstd_dict: zstd.ZstdCompressionDict
    ) -> zstd.ZstdDecompressor:
        self._zstd_decompressors[dict_id] = zstd.ZstdDecompressor(dict_data=zstd_dict)
        return self._zstd_decompressors[dict_id]

    def _add_dict_compressor(
        self,
        dict_id: int,
        zstd_dict: zstd.ZstdCompressionDict,
        compression_params: zstd.ZstdCompressionParameters,
    ) -> zstd.ZstdCompressor:
        self._zstd_compressors[dict_id] = zstd.ZstdCompressor(
            dict_data=zstd_dict, compression_params=compression_params
        )
        return self._zstd_compressors[dict_id]

    def _compress(self, key: Key, data: bytes) -> Tuple[bytes, int]:
        if key.domain and (dict_id := self._domain_to_dict_id.get(key.domain)):
            return self._zstd_compressors[dict_id].compress(data), self.ZSTD_COMPRESSED
        elif self._default_zstd_compressor:
            return self._default_zstd_compressor.compress(data), self.ZSTD_COMPRESSED
        else:
            return zlib.compress(data), self.ZLIB_COMPRESSED

    def _decompress(self, data: bytes) -> bytes:
        data = self.ZSTD_MAGIC + data
        dict_id = zstd.get_frame_parameters(data).dict_id
        if decompressor := self._zstd_decompressors.get(dict_id):
            return decompressor.decompress(data)
        raise ValueError(f"Unknown dictionary id: {dict_id}")

    def _should_compress(self, key: Key, data: bytes) -> bool:
        data_len = len(data)
        if data_len >= self._default_compression_threshold:
            return True
        elif data_len >= self._dict_compression_threshold:
            return bool(key.domain and self._domain_to_dict_id.get(key.domain))
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
            data = self._decompress(data)
            encoding_id ^= self.ZSTD_COMPRESSED

        if encoding_id == self.STR:
            return bytes(data).decode()
        elif encoding_id in (self.INT, self.LONG):
            return int(data)
        elif encoding_id == self.BINARY:
            return bytes(data)
        else:
            return pickle.loads(data)  # noqa: S301
