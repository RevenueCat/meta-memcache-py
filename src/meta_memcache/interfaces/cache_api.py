from typing import Dict, Protocol
from meta_memcache.configuration import ServerAddress
from meta_memcache.connection.pool import PoolCounters
from meta_memcache.events.write_failure_event import WriteFailureEvent
from meta_memcache.interfaces.commands import CommandsProtocol


class CacheApi(CommandsProtocol, Protocol):
    @property
    def on_write_failure(self) -> WriteFailureEvent:
        ...  # pragma: no cover

    @on_write_failure.setter
    def on_write_failure(self, value: WriteFailureEvent) -> None:
        ...  # pragma: no cover

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        ...  # pragma: no cover
