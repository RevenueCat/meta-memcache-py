from abc import ABC, abstractmethod
from typing import Any, NamedTuple

from meta_memcache.protocol import Blob


class EncodedValue(NamedTuple):
    data: bytes
    encoding_id: int


class BaseSerializer(ABC):
    @abstractmethod
    def serialize(
        self,
        value: Any,  # pyre-ignore[2]
    ) -> EncodedValue:
        ...

    @abstractmethod
    def unserialize(self, data: Blob, encoding_id: int) -> Any:  # pyre-ignore[3]
        ...
