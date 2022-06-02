from typing import Callable, List

from meta_memcache.protocol import Key


class WriteFailureEvent(object):
    def __init__(self) -> None:
        self._eventhandlers: List[Callable[[Key], None]] = []

    def __iadd__(self, handler: Callable[[Key], None]) -> "WriteFailureEvent":
        self._eventhandlers.append(handler)
        return self

    def __isub__(self, handler: Callable[[Key], None]) -> "WriteFailureEvent":
        self._eventhandlers.remove(handler)
        return self

    def __call__(self, key: Key) -> None:
        for eventhandler in self._eventhandlers:
            eventhandler(key)
