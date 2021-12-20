from abc import ABC, abstractmethod

from meta_memcache.protocol import Key


class BaseWriteFailureTracker(ABC):
    @abstractmethod
    def add_key(self, key: Key) -> None:
        ...
