import time
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from meta_memcache.configuration import LeasePolicy, RecachePolicy, StalePolicy
from meta_memcache.errors import MemcacheError
from meta_memcache.interfaces.high_level_commands import HighLevelCommandsProtocol
from meta_memcache.interfaces.meta_commands import MetaCommandsProtocol
from meta_memcache.interfaces.router import FailureHandling
from meta_memcache.protocol import (
    Key,
    Miss,
    ReadResponse,
    RequestFlags,
    SetMode,
    Success,
    Value,
    MA_MODE_DEC,
)

T = TypeVar("T")
_REFILL_FAILURE_HANDLING = FailureHandling(track_write_failures=False)
DEFAULT_GET_FLAGS = RequestFlags(
    return_value=True,
    return_ttl=True,
    return_client_flag=True,
    return_last_access=True,
    return_fetched=True,
)
DEFAULT_GET_CAS_FLAGS = RequestFlags(
    return_value=True,
    return_ttl=True,
    return_client_flag=True,
    return_last_access=True,
    return_fetched=True,
    return_cas_token=True,
)


class HighLevelCommandMixinWithMetaCommands(
    HighLevelCommandsProtocol, MetaCommandsProtocol, Protocol
):
    def _get(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        lease_policy: Optional[LeasePolicy] = None,
        recache_policy: Optional[RecachePolicy] = None,
        return_cas_token: bool = False,
    ) -> Optional[Value]:
        ...  # pragma: no cover

    def _process_get_result(
        self, key: Union[Key, str], result: ReadResponse
    ) -> Optional[Value]:
        ...  # pragma: no cover

    def _get_typed_value(
        self,
        key: Union[Key, str],
        value: Any,
        cls: Type[T],
        error_on_type_mismatch: bool = False,
    ) -> Optional[T]:
        ...  # pragma: no cover

    def _get_delta_flags(
        self,
        delta: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
        return_value: bool = False,
    ) -> RequestFlags:
        ...  # pragma: no cover


class HighLevelCommandsMixin:
    def set(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        value: Any,
        ttl: int,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
        stale_policy: Optional[StalePolicy] = None,
        set_mode: SetMode = SetMode.SET,
    ) -> bool:
        key = key if isinstance(key, Key) else Key(key)
        flags = RequestFlags(cache_ttl=ttl)
        if no_reply:
            flags.no_reply = True
        if cas_token is not None:
            flags.cas_token = cas_token
            if stale_policy and stale_policy.mark_stale_on_cas_mismatch:
                flags.mark_stale = True
        if set_mode != SetMode.SET:
            flags.mode = set_mode.value

        result = self.meta_set(
            key=key,
            value=value,
            ttl=ttl,
            flags=flags,
        )

        return isinstance(result, Success)

    def refill(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        value: Any,
        ttl: int,
        no_reply: bool = False,
    ) -> bool:
        """
        Try to refill a value.

        Use this method when you got a cache miss, read from DB and
        are trying to refill the value.

        DO NOT USE to write new state.

        It will:
         * use "ADD" mode, so it will fail if the value is already
           present in cache.
         * It will also disable write failure tracking. The write
           failure tracking is often used to invalidate keys that
           fail to be written. Since this is not writting new state,
           there is no need to track failures.
        """
        key = key if isinstance(key, Key) else Key(key)
        flags = RequestFlags(
            cache_ttl=ttl,
            mode=SetMode.ADD.value,
        )
        if no_reply:
            flags.no_reply = True

        result = self.meta_set(
            key=key,
            value=value,
            ttl=ttl,
            flags=flags,
            failure_handling=_REFILL_FAILURE_HANDLING,
        )

        return isinstance(result, Success)

    def delete(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        cas_token: Optional[int] = None,
        no_reply: bool = False,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:
        """
        Returns True if the key existed and it was deleted.
        If the key is not found in the cache it will return False. If
        you just want to the key to be deleted not caring of whether
        it exists or not, use invalidate() instead.
        """
        key = key if isinstance(key, Key) else Key(key)
        flags = RequestFlags()
        if no_reply:
            flags.no_reply = True
        if cas_token is not None:
            flags.cas_token = cas_token
        if stale_policy and stale_policy.mark_stale_on_deletion_ttl > 0:
            flags.mark_stale = True
            flags.cache_ttl = stale_policy.mark_stale_on_deletion_ttl

        result = self.meta_delete(key=key, flags=flags)

        return isinstance(result, Success)

    def invalidate(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        cas_token: Optional[int] = None,
        no_reply: bool = False,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:
        """
        Returns true of the key deleted or it didn't exist anyway
        """
        key = key if isinstance(key, Key) else Key(key)
        flags = RequestFlags()
        if no_reply:
            flags.no_reply = True
        if cas_token is not None:
            flags.cas_token = cas_token
        if stale_policy and stale_policy.mark_stale_on_deletion_ttl > 0:
            flags.mark_stale = True
            flags.cache_ttl = stale_policy.mark_stale_on_deletion_ttl

        result = self.meta_delete(key=key, flags=flags)

        return isinstance(result, (Success, Miss))

    def touch(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        ttl: int,
        no_reply: bool = False,
    ) -> bool:
        key = key if isinstance(key, Key) else Key(key)
        flags = RequestFlags(cache_ttl=ttl)
        if no_reply:
            flags.no_reply = True
        result = self.meta_get(key, flags=flags)

        return isinstance(result, Success)

    def get_or_lease(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        value, _ = self.get_or_lease_cas(
            key=key,
            lease_policy=lease_policy,
            touch_ttl=touch_ttl,
            recache_policy=recache_policy,
        )
        return value

    def get_or_lease_cas(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        if lease_policy.miss_retries <= 0:
            raise ValueError(
                "Wrong lease_policy: miss_retries needs to be greater than 0"
            )
        i = 0
        while True:
            if i > 0:
                time.sleep(
                    min(
                        lease_policy.miss_max_retry_wait,
                        lease_policy.miss_retry_wait
                        * pow(lease_policy.wait_backoff_factor, i - 1),
                    )
                )
            i += 1

            result = self._get(
                key=key,
                touch_ttl=touch_ttl,
                lease_policy=lease_policy,
                recache_policy=recache_policy,
                return_cas_token=True,
            )

            if isinstance(result, Value):
                # It is a hit.
                if result.flags.win:
                    # Win flag present, meaning we got the lease to
                    # recache/cache the item. We need to mimic a miss.
                    return None, result.flags.cas_token
                if result.size == 0 and result.flags.win is False:
                    # The value is empty, this is a miss lease,
                    # and we lost, so we must keep retrying and
                    # wait for the.flags.winner to populate the value.
                    if i < lease_policy.miss_retries:
                        continue
                    else:
                        # We run out of retries, behave as a miss
                        return None, result.flags.cas_token
                else:
                    # There is data, either the is no lease or
                    # we lost and should use the stale value.
                    return result.value, result.flags.cas_token
            else:
                # With MISS_LEASE_TTL we should always get a value
                # because on miss a lease empty value is generated
                raise MemcacheError(f"Unexpected response: {result} for key {key}")

    def get(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        result = self._get(
            key=key,
            touch_ttl=touch_ttl,
            recache_policy=recache_policy,
            return_cas_token=False,
        )
        return result.value if result is not None else None

    def multi_get(
        self: HighLevelCommandMixinWithMetaCommands,
        keys: Iterable[Union[Key, str]],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Dict[Key, Optional[Any]]:
        results = self._multi_get(
            keys=keys,
            touch_ttl=touch_ttl,
            recache_policy=recache_policy,
        )
        return {k: v.value if v is not None else None for k, v in results.items()}

    def _multi_get(
        self: HighLevelCommandMixinWithMetaCommands,
        keys: Iterable[Union[Key, str]],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        return_cas_token: bool = False,
    ) -> Dict[Key, Optional[Value]]:
        if return_cas_token:
            flags = DEFAULT_GET_CAS_FLAGS.copy()
        else:
            flags = DEFAULT_GET_FLAGS.copy()
        if recache_policy:
            flags.recache_ttl = recache_policy.ttl
        if touch_ttl is not None and touch_ttl >= 0:
            flags.cache_ttl = touch_ttl

        results = self.meta_multiget(
            keys=[key if isinstance(key, Key) else Key(key) for key in keys],
            flags=flags,
        )
        return {k: self._process_get_result(k, v) for k, v in results.items()}

    def get_cas(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        result = self._get(
            key=key,
            touch_ttl=touch_ttl,
            recache_policy=recache_policy,
            return_cas_token=True,
        )
        if result is None:
            return None, None
        else:
            return result.value, result.flags.cas_token

    def _get(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        lease_policy: Optional[LeasePolicy] = None,
        recache_policy: Optional[RecachePolicy] = None,
        return_cas_token: bool = False,
    ) -> Optional[Value]:
        key = key if isinstance(key, Key) else Key(key)
        if return_cas_token:
            flags = DEFAULT_GET_CAS_FLAGS.copy()
        else:
            flags = DEFAULT_GET_FLAGS.copy()
        if lease_policy:
            flags.vivify_on_miss_ttl = lease_policy.ttl
        if recache_policy:
            flags.recache_ttl = recache_policy.ttl
        if touch_ttl is not None and touch_ttl >= 0:
            flags.cache_ttl = touch_ttl

        result = self.meta_get(key, flags=flags)
        return self._process_get_result(key, result)

    def _process_get_result(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        result: ReadResponse,
    ) -> Optional[Value]:
        if isinstance(result, Value):
            # It is a hit
            if result.flags.win:
                # Win flag present, meaning we got the lease to
                # recache the item. We need to mimic a miss, so
                # we set the value to None.
                result.value = None
            return result
        elif isinstance(result, Miss):
            return None
        else:
            raise MemcacheError(f"Unexpected response: {result} for key {key}")

    def get_typed(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Optional[T]:
        value = self.get(
            key=key,
            touch_ttl=touch_ttl,
            recache_policy=recache_policy,
        )
        return self._get_typed_value(key, value, cls, error_on_type_mismatch)

    def get_cas_typed(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Tuple[Optional[T], Optional[int]]:
        value, cas_token = self.get_cas(
            key=key, touch_ttl=touch_ttl, recache_policy=recache_policy
        )
        return self._get_typed_value(key, value, cls, error_on_type_mismatch), cas_token

    def _get_typed_value(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        value: Any,
        cls: Type[T],
        error_on_type_mismatch: bool = False,
    ) -> Optional[T]:
        result: Optional[T] = None
        if value is not None:
            if isinstance(value, cls):
                result = value
            elif error_on_type_mismatch:
                raise ValueError(f"Expecting {cls} got {value} for {key}")
        return result

    def _get_delta_flags(
        self: HighLevelCommandMixinWithMetaCommands,
        delta: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
        return_value: bool = False,
    ) -> RequestFlags:
        flags = RequestFlags(
            ma_delta_value=abs(delta),
        )
        if return_value:
            flags.return_value = True
        if no_reply:
            flags.no_reply = True
        if refresh_ttl is not None:
            flags.cache_ttl = refresh_ttl
        if cas_token is not None:
            flags.cas_token = cas_token
        if delta < 0:
            flags.mode = MA_MODE_DEC

        return flags

    def delta(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        delta: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
    ) -> bool:
        key = key if isinstance(key, Key) else Key(key)
        flags = self._get_delta_flags(
            delta=delta,
            refresh_ttl=refresh_ttl,
            no_reply=no_reply,
            cas_token=cas_token,
        )
        result = self.meta_arithmetic(key=key, flags=flags)
        return isinstance(result, Success)

    def delta_initialize(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        delta: int,
        initial_value: int,
        initial_ttl: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
    ) -> bool:
        key = key if isinstance(key, Key) else Key(key)
        flags = self._get_delta_flags(
            delta=delta,
            refresh_ttl=refresh_ttl,
            no_reply=no_reply,
            cas_token=cas_token,
        )
        flags.ma_initial_value = abs(initial_value)
        flags.vivify_on_miss_ttl = initial_ttl
        result = self.meta_arithmetic(key=key, flags=flags)
        return isinstance(result, Success)

    def delta_and_get(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        delta: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:
        key = key if isinstance(key, Key) else Key(key)
        flags = self._get_delta_flags(
            return_value=True,
            delta=delta,
            refresh_ttl=refresh_ttl,
            cas_token=cas_token,
        )
        result = self.meta_arithmetic(key=key, flags=flags)
        if isinstance(result, Value):
            if isinstance(result.value, str) and result.value.isnumeric():
                return int(result.value)
            else:
                raise MemcacheError(
                    f"Unexpected value from meta arithmetic command: {result.value}"
                )
        return None

    def delta_initialize_and_get(
        self: HighLevelCommandMixinWithMetaCommands,
        key: Union[Key, str],
        delta: int,
        initial_value: int,
        initial_ttl: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:
        key = key if isinstance(key, Key) else Key(key)
        flags = self._get_delta_flags(
            return_value=True,
            delta=delta,
            refresh_ttl=refresh_ttl,
            cas_token=cas_token,
        )
        flags.ma_initial_value = abs(initial_value)
        flags.vivify_on_miss_ttl = initial_ttl
        result = self.meta_arithmetic(key=key, flags=flags)
        if isinstance(result, Value):
            if isinstance(result.value, str) and result.value.isnumeric():
                return int(result.value)
            else:
                raise MemcacheError(
                    f"Unexpected value from meta arithmetic command: {result.value}"
                )
        return None
