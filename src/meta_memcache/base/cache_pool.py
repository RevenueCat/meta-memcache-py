import time
from typing import Any, Optional, Tuple, Type, TypeVar

from meta_memcache.base.base_cache_pool import BaseCachePool
from meta_memcache.configuration import LeasePolicy, RecachePolicy, StalePolicy
from meta_memcache.errors import MemcacheError
from meta_memcache.protocol import Flag, IntFlag, Key, Miss, Success, Value

T = TypeVar("T")


class CachePool(BaseCachePool):
    def set(
        self,
        key: Key,
        value: Any,  # pyre-ignore[2]
        ttl: int,
        no_reply: bool = False,
        cas: Optional[int] = None,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:
        flags = set()
        if no_reply:
            flags.add(Flag.NOREPLY)
        int_flags = {
            IntFlag.CACHE_TTL: ttl,
        }
        if cas is not None:
            int_flags[IntFlag.CAS] = cas
            if stale_policy and stale_policy.mark_stale_on_cas_mismatch:
                flags.add(Flag.MARK_STALE)

        result = self.ms(
            key=key,
            value=value,
            ttl=ttl,
            flags=flags,
            int_flags=int_flags,
        )

        return isinstance(result, Success)

    def delete(
        self,
        key: Key,
        cas: Optional[int] = None,
        no_reply: bool = False,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:
        flags = set()
        int_flags = {}
        if no_reply:
            flags.add(Flag.NOREPLY)
        if cas is not None:
            int_flags[IntFlag.CAS] = cas
        if stale_policy and stale_policy.mark_stale_on_deletion_ttl > 0:
            flags.add(Flag.MARK_STALE)
            int_flags[IntFlag.CACHE_TTL] = stale_policy.mark_stale_on_deletion_ttl

        result = self.md(
            key=key,
            flags=flags,
            int_flags=int_flags,
        )

        return isinstance(result, Success)

    def touch(self, key: Key, ttl: int, no_reply: bool = False) -> bool:
        flags = set()
        int_flags = {IntFlag.CACHE_TTL: ttl}
        if no_reply:
            flags.add(Flag.NOREPLY)
        result = self.mg(key, flags=flags, int_flags=int_flags)

        return isinstance(result, Success)

    # pyre-ignore[3]
    def get_or_lease(
        self,
        key: Key,
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

    # pyre-ignore[3]
    def get_or_lease_cas(
        self,
        key: Key,
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        if lease_policy.miss_retries <= 0:
            raise ValueError(
                "Wrong lease_policy: miss_retries needs to be greater than 0"
            )
        flags = {
            Flag.RETURN_VALUE,
            Flag.RETURN_TTL,
            Flag.RETURN_CLIENT_FLAG,
            Flag.RETURN_CAS,
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
                print(
                    "sleep",
                    min(
                        lease_policy.miss_max_retry_wait,
                        lease_policy.miss_retry_wait
                        * pow(lease_policy.wait_backoff_factor, i - 1),
                    ),
                )
                print(time.time)
                time.sleep(
                    min(
                        lease_policy.miss_max_retry_wait,
                        lease_policy.miss_retry_wait
                        * pow(lease_policy.wait_backoff_factor, i - 1),
                    )
                )
            i += 1

            result = self.mg(key, flags=flags, int_flags=int_flags)
            print(result)

            if isinstance(result, Value):
                # It is a hit.
                cas = result.int_flags.get(IntFlag.CAS)
                if Flag.WIN in result.flags:
                    # Win flag present, meaning we got the lease to
                    # recache/cache the item. We need to mimic a miss.
                    return None, cas
                if result.size == 0 and Flag.LOST in result.flags:
                    # The value is empty, this is a miss lease,
                    # and we lost, so we must keep retrying and
                    # wait for the winner to populate the value.
                    if i < lease_policy.miss_retries:
                        continue
                    else:
                        # We run of retries, behave as a miss
                        return None, cas
                else:
                    # There is data, either the is no lease or
                    # we lost and should use the stale value.
                    return result.value, cas
            else:
                # With MISS_LEASE_TTL we should always get a value
                # because on miss a lease empty value is generated
                raise MemcacheError("Unexpected response")

    # pyre-ignore[3]
    def get(
        self,
        key: Key,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        value, _ = self.get_cas(
            key=key, touch_ttl=touch_ttl, recache_policy=recache_policy
        )
        return value

    # pyre-ignore[3]
    def get_cas(
        self,
        key: Key,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        flags = {
            Flag.RETURN_VALUE,
            Flag.RETURN_TTL,
            Flag.RETURN_CLIENT_FLAG,
            Flag.RETURN_CAS,
            Flag.RETURN_LAST_ACCESS,
            Flag.RETURN_FETCHED,
        }
        int_flags = {}
        if recache_policy:
            int_flags[IntFlag.RECACHE_TTL] = recache_policy.ttl
        if touch_ttl is not None and touch_ttl >= 0:
            int_flags[IntFlag.CACHE_TTL] = touch_ttl

        result = self.mg(key, flags=flags, int_flags=int_flags)
        if isinstance(result, Value):
            # It is a hit
            cas = result.int_flags.get(IntFlag.CAS)
            if Flag.WIN in result.flags:
                # Win flag present, meaning we got the lease to
                # recache the item. We need to mimic a miss.
                return None, cas
            else:
                return result.value, cas
        elif isinstance(result, Miss):
            return None, None
        else:
            raise MemcacheError("Unexpected response")

    def get_typed(
        self,
        key: Key,
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
        key: Key,
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Tuple[Optional[T], Optional[int]]:
        value, cas = self.get_cas(
            key=key, touch_ttl=touch_ttl, recache_policy=recache_policy
        )

        if not isinstance(value, cls):
            if error_on_type_mismatch:
                raise ValueError(f"Expecting {cls} got {value}")
            else:
                value = None
        return value, cas
