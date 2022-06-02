import time
from enum import Enum
from typing import Any, Dict, Iterable, Optional, Set, Tuple, Type, TypeVar, Union

from meta_memcache.base.base_cache_pool import BaseCachePool
from meta_memcache.configuration import LeasePolicy, RecachePolicy, StalePolicy
from meta_memcache.errors import MemcacheError
from meta_memcache.protocol import Flag, IntFlag, Key, Miss, Success, TokenFlag, Value

T = TypeVar("T")


class SetMode(Enum):
    SET = b"S"  # Default
    ADD = b"E"  # LRU bump and return NS if item exists. Else add.
    APPEND = b"A"  # If item exists, append the new value to its data.
    PREPEND = b"P"  # If item exists, prepend the new value to its data.
    REPLACE = b"R"  # Set only if item already exists.


class CachePool(BaseCachePool):
    def set(
        self,
        key: Union[Key, str],
        value: Any,
        ttl: int,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
        stale_policy: Optional[StalePolicy] = None,
        set_mode: SetMode = SetMode.SET,
    ) -> bool:
        key = key if isinstance(key, Key) else Key(key)
        flags = set()
        if no_reply:
            flags.add(Flag.NOREPLY)
        int_flags = {
            IntFlag.CACHE_TTL: ttl,
        }
        if cas_token is not None:
            int_flags[IntFlag.CAS_TOKEN] = cas_token
            if stale_policy and stale_policy.mark_stale_on_cas_mismatch:
                flags.add(Flag.MARK_STALE)
        if set_mode == SetMode.SET:
            token_flags = None
        else:
            token_flags = {TokenFlag.MODE: set_mode.value}

        result = self.meta_set(
            key=key,
            value=value,
            ttl=ttl,
            flags=flags,
            int_flags=int_flags,
            token_flags=token_flags,
        )

        return isinstance(result, Success)

    def delete(
        self,
        key: Union[Key, str],
        cas_token: Optional[int] = None,
        no_reply: bool = False,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:
        key = key if isinstance(key, Key) else Key(key)
        flags = set()
        int_flags = {}
        if no_reply:
            flags.add(Flag.NOREPLY)
        if cas_token is not None:
            int_flags[IntFlag.CAS_TOKEN] = cas_token
        if stale_policy and stale_policy.mark_stale_on_deletion_ttl > 0:
            flags.add(Flag.MARK_STALE)
            int_flags[IntFlag.CACHE_TTL] = stale_policy.mark_stale_on_deletion_ttl

        result = self.meta_delete(
            key=key,
            flags=flags,
            int_flags=int_flags,
        )

        return isinstance(result, Success)

    def touch(
        self,
        key: Union[Key, str],
        ttl: int,
        no_reply: bool = False,
    ) -> bool:
        key = key if isinstance(key, Key) else Key(key)
        flags = set()
        int_flags = {IntFlag.CACHE_TTL: ttl}
        if no_reply:
            flags.add(Flag.NOREPLY)
        result = self.meta_get(key, flags=flags, int_flags=int_flags)

        return isinstance(result, Success)

    def get_or_lease(
        self,
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
        self,
        key: Union[Key, str],
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        key = key if isinstance(key, Key) else Key(key)
        if lease_policy.miss_retries <= 0:
            raise ValueError(
                "Wrong lease_policy: miss_retries needs to be greater than 0"
            )
        flags = {
            Flag.RETURN_VALUE,
            Flag.RETURN_TTL,
            Flag.RETURN_CLIENT_FLAG,
            Flag.RETURN_CAS_TOKEN,
            Flag.RETURN_LAST_ACCESS,
            Flag.RETURN_FETCHED,
        }
        int_flags = {
            IntFlag.MISS_LEASE_TTL: lease_policy.ttl,
        }
        if recache_policy:
            int_flags[IntFlag.RECACHE_TTL] = recache_policy.ttl
        if touch_ttl is not None and touch_ttl >= 0:
            int_flags[IntFlag.CACHE_TTL] = touch_ttl
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

            result = self.meta_get(key, flags=flags, int_flags=int_flags)

            if isinstance(result, Value):
                # It is a hit.
                cas_token = result.int_flags.get(IntFlag.RETURNED_CAS_TOKEN)
                if Flag.WIN in result.flags:
                    # Win flag present, meaning we got the lease to
                    # recache/cache the item. We need to mimic a miss.
                    return None, cas_token
                if result.size == 0 and Flag.LOST in result.flags:
                    # The value is empty, this is a miss lease,
                    # and we lost, so we must keep retrying and
                    # wait for the winner to populate the value.
                    if i < lease_policy.miss_retries:
                        continue
                    else:
                        # We run of retries, behave as a miss
                        return None, cas_token
                else:
                    # There is data, either the is no lease or
                    # we lost and should use the stale value.
                    return result.value, cas_token
            else:
                # With MISS_LEASE_TTL we should always get a value
                # because on miss a lease empty value is generated
                raise MemcacheError(f"Unexpected response: {result} for key {key}")

    def get(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        value, _ = self.get_cas(
            key=key, touch_ttl=touch_ttl, recache_policy=recache_policy
        )
        return value

    def multi_get(
        self,
        keys: Iterable[Union[Key, str]],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Dict[Key, Optional[Any]]:
        flags = {
            Flag.RETURN_VALUE,
            Flag.RETURN_TTL,
            Flag.RETURN_CLIENT_FLAG,
            Flag.RETURN_LAST_ACCESS,
            Flag.RETURN_FETCHED,
        }
        int_flags = {}
        if recache_policy:
            int_flags[IntFlag.RECACHE_TTL] = recache_policy.ttl
        if touch_ttl is not None and touch_ttl >= 0:
            int_flags[IntFlag.CACHE_TTL] = touch_ttl

        results: Dict[Key, Optional[Any]] = {}
        for key, result in self.meta_multiget(
            keys=[key if isinstance(key, Key) else Key(key) for key in keys],
            flags=flags,
            int_flags=int_flags,
        ).items():
            if isinstance(result, Value):
                # It is a hit
                if Flag.WIN in result.flags:
                    # Win flag present, meaning we got the lease to
                    # recache the item. We need to mimic a miss.
                    results[key] = None
                else:
                    results[key] = result.value
            elif isinstance(result, Miss):
                results[key] = None
            else:
                raise MemcacheError(f"Unexpected response: {result} for key {key}")
        return results

    def get_cas(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        key = key if isinstance(key, Key) else Key(key)
        flags = {
            Flag.RETURN_VALUE,
            Flag.RETURN_TTL,
            Flag.RETURN_CLIENT_FLAG,
            Flag.RETURN_CAS_TOKEN,
            Flag.RETURN_LAST_ACCESS,
            Flag.RETURN_FETCHED,
        }
        int_flags = {}
        if recache_policy:
            int_flags[IntFlag.RECACHE_TTL] = recache_policy.ttl
        if touch_ttl is not None and touch_ttl >= 0:
            int_flags[IntFlag.CACHE_TTL] = touch_ttl

        result = self.meta_get(key, flags=flags, int_flags=int_flags)
        if isinstance(result, Value):
            # It is a hit
            cas_token = result.int_flags.get(IntFlag.RETURNED_CAS_TOKEN)
            if Flag.WIN in result.flags:
                # Win flag present, meaning we got the lease to
                # recache the item. We need to mimic a miss.
                return None, cas_token
            else:
                return result.value, cas_token
        elif isinstance(result, Miss):
            return None, None
        else:
            raise MemcacheError(f"Unexpected response: {result} for key {key}")

    def get_typed(
        self,
        key: Union[Key, str],
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Optional[T]:
        value, _ = self.get_cas_typed(
            key=key,
            cls=cls,
            touch_ttl=touch_ttl,
            recache_policy=recache_policy,
            error_on_type_mismatch=error_on_type_mismatch,
        )
        return value

    def get_cas_typed(
        self,
        key: Union[Key, str],
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Tuple[Optional[T], Optional[int]]:
        value, cas_token = self.get_cas(
            key=key, touch_ttl=touch_ttl, recache_policy=recache_policy
        )

        if not isinstance(value, cls):
            if error_on_type_mismatch:
                raise ValueError(f"Expecting {cls} got {value} for {key}")
            else:
                value = None
        return value, cas_token

    def _get_delta_flags(
        self,
        delta: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
        return_value: bool = False,
    ) -> Tuple[Set[Flag], Dict[IntFlag, int], Dict[TokenFlag, bytes]]:
        flags = set()
        int_flags = {
            IntFlag.MA_DELTA_VALUE: abs(delta),
        }
        token_flags = {}

        if return_value:
            flags.add(Flag.RETURN_VALUE)
        if no_reply:
            flags.add(Flag.NOREPLY)
        if refresh_ttl is not None:
            int_flags[IntFlag.CACHE_TTL] = refresh_ttl
        if cas_token is not None:
            int_flags[IntFlag.CAS_TOKEN] = cas_token
        if delta < 0:
            token_flags[TokenFlag.MODE] = b"-"

        return flags, int_flags, token_flags

    def delta(
        self,
        key: Union[Key, str],
        delta: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
    ) -> bool:
        key = key if isinstance(key, Key) else Key(key)
        flags, int_flags, token_flags = self._get_delta_flags(
            delta=delta,
            refresh_ttl=refresh_ttl,
            no_reply=no_reply,
            cas_token=cas_token,
        )
        result = self.meta_arithmetic(
            key=key, flags=flags, int_flags=int_flags, token_flags=token_flags
        )
        return isinstance(result, Success)

    def delta_initialize(
        self,
        key: Union[Key, str],
        delta: int,
        initial_value: int,
        initial_ttl: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
    ) -> bool:
        key = key if isinstance(key, Key) else Key(key)
        flags, int_flags, token_flags = self._get_delta_flags(
            delta=delta,
            refresh_ttl=refresh_ttl,
            no_reply=no_reply,
            cas_token=cas_token,
        )
        int_flags[IntFlag.MA_INITIAL_VALUE] = abs(initial_value)
        int_flags[IntFlag.MISS_LEASE_TTL] = initial_ttl
        result = self.meta_arithmetic(
            key=key, flags=flags, int_flags=int_flags, token_flags=token_flags
        )
        return isinstance(result, Success)

    def delta_and_get(
        self,
        key: Union[Key, str],
        delta: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:
        key = key if isinstance(key, Key) else Key(key)
        flags, int_flags, token_flags = self._get_delta_flags(
            return_value=True,
            delta=delta,
            refresh_ttl=refresh_ttl,
            cas_token=cas_token,
        )
        result = self.meta_arithmetic(
            key=key, flags=flags, int_flags=int_flags, token_flags=token_flags
        )
        if isinstance(result, Value):
            if isinstance(result.value, str) and result.value.isnumeric():
                return int(result.value)
            else:
                raise MemcacheError(
                    f"Unexpected value from meta arithmetic command: {result.value}"
                )
        return None

    def delta_initialize_and_get(
        self,
        key: Union[Key, str],
        delta: int,
        initial_value: int,
        initial_ttl: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:
        key = key if isinstance(key, Key) else Key(key)
        flags, int_flags, token_flags = self._get_delta_flags(
            return_value=True,
            delta=delta,
            refresh_ttl=refresh_ttl,
            cas_token=cas_token,
        )
        int_flags[IntFlag.MA_INITIAL_VALUE] = abs(initial_value)
        int_flags[IntFlag.MISS_LEASE_TTL] = initial_ttl
        result = self.meta_arithmetic(
            key=key, flags=flags, int_flags=int_flags, token_flags=token_flags
        )
        if isinstance(result, Value):
            if isinstance(result.value, str) and result.value.isnumeric():
                return int(result.value)
            else:
                raise MemcacheError(
                    f"Unexpected value from meta arithmetic command: {result.value}"
                )
        return None
