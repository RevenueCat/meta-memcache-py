from typing import Dict
from meta_memcache.configuration import ServerAddress
from meta_memcache.connection.pool import PoolCounters
from meta_memcache.events.write_failure_event import WriteFailureEvent
from meta_memcache.interfaces.router import Router


class BaseCacheClient:
    def __init__(
        self,
        router: Router,
    ):
        self.router = router

    @property
    def on_write_failure(self) -> WriteFailureEvent:
        return self.router.executor.on_write_failure

    @on_write_failure.setter
    def on_write_failure(self, value: WriteFailureEvent) -> None:
        self.router.executor.on_write_failure = value

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        return self.router.get_counters()
