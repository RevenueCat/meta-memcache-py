import random
import time
from typing import Any, Dict, List, Optional, Set, Union

from meta_memcache.commands.high_level_commands import HighLevelCommandsMixin
from meta_memcache.configuration import MigrationMode, ServerAddress
from meta_memcache.connection.pool import PoolCounters
from meta_memcache.events.write_failure_event import WriteFailureEvent
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.protocol import (
    Flag,
    IntFlag,
    Key,
    ReadResponse,
    SetMode,
    TokenFlag,
    Value,
    WriteResponse,
)


class MigratingCacheClient(HighLevelCommandsMixin):
    """
    Cache pool that migrates data from one cache to another.

    This is useful for testing read paths while the write
    path (and thus the invalidations and cache updates)
    are nor enabled, so expiring data is the only way
    to keep cache consistent.

    migration_mode_config controls how the pools are used.
    See MigrationMode for description on the different modes.
    This can be just a single MigrationMode or a dict of
    MigrationMode to unix timestamp, controlling when each
    of the modes should be used.
    For example
    {
        MigrationMode.POPULATE_WRITES: 10,
        MigrationMode.USE_DESTINATION_UPDATE_ORIGIN: 20,
        MigrationMode.ONLY_DESTINATION: 30
    }
    will use the default, ONLY_ORIGIN, until time=10,
    then POPULATE_WRITES until time=20 and then ONLY_DESTINATION
    after time=30.
    """

    def __init__(
        self,
        origin_client: CacheApi,
        destination_client: CacheApi,
        migration_mode_config: Union[MigrationMode, Dict[MigrationMode, int]],
        default_read_backfill_ttl: int = 3600,
    ) -> None:
        # Share the WriteFailureEvent across all clients
        self.on_write_failure = WriteFailureEvent()
        origin_client.on_write_failure = self.on_write_failure
        destination_client.on_write_failure = self.on_write_failure

        self._origin_client: CacheApi = origin_client
        self._destination_client: CacheApi = destination_client
        self._migration_mode_config = migration_mode_config
        self._default_read_backfill_ttl = default_read_backfill_ttl

    def get_migration_mode(self) -> MigrationMode:
        if isinstance(self._migration_mode_config, MigrationMode):
            return self._migration_mode_config
        else:
            now = time.time()
            current_until = -1
            current_mode = MigrationMode.ONLY_ORIGIN
            for mode, until in self._migration_mode_config.items():
                if now >= until and until > current_until:
                    current_until = until
                    current_mode = mode
            return current_mode

    def _get_value_ttl(self, value: Value) -> int:
        ttl = value.int_flags.get(IntFlag.TTL, self._default_read_backfill_ttl)
        if ttl < 0:
            # TTL for items marked to store forvered is returned as -1
            ttl = 0
        return ttl

    def _should_populate_read(self, migration_mode: MigrationMode) -> bool:
        pct = (
            1 if migration_mode == MigrationMode.POPULATE_WRITES_AND_READS_1PCT else 10
        )
        return (random.getrandbits(10) % 100) < pct

    def meta_multiget(
        self,
        keys: List[Key],
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> Dict[Key, ReadResponse]:
        migration_mode = self.get_migration_mode()
        if migration_mode >= MigrationMode.USE_DESTINATION_UPDATE_ORIGIN:
            return self._destination_client.meta_multiget(
                keys=keys,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
        elif migration_mode in (
            MigrationMode.POPULATE_WRITES_AND_READS_1PCT,
            MigrationMode.POPULATE_WRITES_AND_READS_10PCT,
        ):
            results = self._origin_client.meta_multiget(
                keys=keys,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
            if self._should_populate_read(migration_mode):
                for key, result in results.items():
                    if isinstance(result, Value):
                        self._destination_client.set(
                            key=key,
                            value=result.value,
                            ttl=self._get_value_ttl(result),
                            no_reply=True,
                            set_mode=SetMode.ADD,
                        )
            return results
        else:
            return self._origin_client.meta_multiget(
                keys=keys,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )

    def meta_get(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> ReadResponse:
        migration_mode = self.get_migration_mode()
        if migration_mode >= MigrationMode.USE_DESTINATION_UPDATE_ORIGIN:
            return self._destination_client.meta_get(
                key=key,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
        elif migration_mode in (
            MigrationMode.POPULATE_WRITES_AND_READS_1PCT,
            MigrationMode.POPULATE_WRITES_AND_READS_10PCT,
        ):
            result = self._origin_client.meta_get(
                key=key,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
            if isinstance(result, Value) and self._should_populate_read(migration_mode):
                self._destination_client.set(
                    key=key,
                    value=result.value,
                    ttl=self._get_value_ttl(result),
                    no_reply=True,
                    set_mode=SetMode.ADD,
                )
            return result
        else:
            return self._origin_client.meta_get(
                key=key,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )

    def meta_set(
        self,
        key: Key,
        value: Any,
        ttl: int,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        origin_response = destination_response = None
        migration_mode = self.get_migration_mode()
        if migration_mode < MigrationMode.ONLY_DESTINATION:
            origin_response = self._origin_client.meta_set(
                key=key,
                value=value,
                ttl=ttl,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
        if migration_mode > MigrationMode.ONLY_ORIGIN:
            destination_response = self._destination_client.meta_set(
                key=key,
                value=value,
                ttl=ttl,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
        if migration_mode >= MigrationMode.USE_DESTINATION_UPDATE_ORIGIN:
            assert destination_response is not None  # noqa: S101
            return destination_response
        else:
            assert origin_response is not None  # noqa: S101
            return origin_response

    def meta_delete(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        origin_response = destination_response = None
        migration_mode = self.get_migration_mode()
        if migration_mode < MigrationMode.ONLY_DESTINATION:
            origin_response = self._origin_client.meta_delete(
                key=key,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
        if migration_mode > MigrationMode.ONLY_ORIGIN:
            destination_response = self._destination_client.meta_delete(
                key=key,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
        if migration_mode >= MigrationMode.USE_DESTINATION_UPDATE_ORIGIN:
            assert destination_response is not None  # noqa: S101
            return destination_response
        else:
            assert origin_response is not None  # noqa: S101
            return origin_response

    def meta_arithmetic(
        self,
        key: Key,
        flags: Optional[Set[Flag]] = None,
        int_flags: Optional[Dict[IntFlag, int]] = None,
        token_flags: Optional[Dict[TokenFlag, bytes]] = None,
    ) -> WriteResponse:
        """
        We can't reliably migrate cache data modified by meta-arithmetic,
        since we may now know the value after the operation. Using memcache
        for counters is only used as rough estimates, so we can just ignore
        this for the migration.
        """
        migration_mode = self.get_migration_mode()
        if migration_mode >= MigrationMode.USE_DESTINATION_UPDATE_ORIGIN:
            return self._destination_client.meta_arithmetic(
                key=key,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )
        else:
            return self._origin_client.meta_arithmetic(
                key=key,
                flags=flags,
                int_flags=int_flags,
                token_flags=token_flags,
            )

    def touch(
        self,
        key: Union[Key, str],
        ttl: int,
        no_reply: bool = False,
    ) -> bool:
        """
        This is a special kind of get that also updates the ttl of the key, and
        it is used to ensure cache consistency, so we will send it to both clients.
        """
        origin_response = destination_response = None
        migration_mode = self.get_migration_mode()
        if migration_mode < MigrationMode.ONLY_DESTINATION:
            origin_response = self._origin_client.touch(
                key=key, ttl=ttl, no_reply=no_reply
            )
        if migration_mode > MigrationMode.ONLY_ORIGIN:
            destination_response = self._destination_client.touch(
                key=key, ttl=ttl, no_reply=no_reply
            )
        if migration_mode >= MigrationMode.USE_DESTINATION_UPDATE_ORIGIN:
            assert destination_response is not None  # noqa: S101
            return destination_response
        else:
            assert origin_response is not None  # noqa: S101
            return origin_response

    def get_counters(self) -> Dict[ServerAddress, PoolCounters]:
        counters = self._origin_client.get_counters()
        counters.update(self._destination_client.get_counters())
        return counters
