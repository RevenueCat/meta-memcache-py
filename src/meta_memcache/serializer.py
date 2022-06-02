import pickle  # noqa: S403
import zlib
from typing import Any

from meta_memcache.base.base_serializer import BaseSerializer, EncodedValue
from meta_memcache.protocol import Blob


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
