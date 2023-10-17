from typing import Any, Dict, Iterable, Optional, Protocol, Tuple, Type, TypeVar, Union

from meta_memcache.configuration import LeasePolicy, RecachePolicy, StalePolicy
from meta_memcache.protocol import Key, SetMode, Value

T = TypeVar("T")


class HighLevelCommandsProtocol(Protocol):
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
        ...  # pragma: no cover

    def refill(
        self,
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
        ...  # pragma: no cover

    def delete(
        self,
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
        ...  # pragma: no cover

    def invalidate(
        self,
        key: Union[Key, str],
        cas_token: Optional[int] = None,
        no_reply: bool = False,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:
        """
        Returns true of the key deleted or it didn't exist anyway
        """
        ...  # pragma: no cover

    def touch(
        self,
        key: Union[Key, str],
        ttl: int,
        no_reply: bool = False,
    ) -> bool:
        ...  # pragma: no cover

    def get_or_lease(
        self,
        key: Union[Key, str],
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        ...  # pragma: no cover

    def get_or_lease_cas(
        self,
        key: Union[Key, str],
        lease_policy: LeasePolicy,
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        ...  # pragma: no cover

    def get(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Optional[Any]:
        ...  # pragma: no cover

    def multi_get(
        self,
        keys: Iterable[Union[Key, str]],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Dict[Key, Optional[Any]]:
        ...  # pragma: no cover

    def _multi_get(
        self,
        keys: Iterable[Union[Key, str]],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        return_cas_token: bool = False,
    ) -> Dict[Key, Optional[Value]]:
        ...  # pragma: no cover

    def get_cas(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
    ) -> Tuple[Optional[Any], Optional[int]]:
        ...  # pragma: no cover

    def _get(
        self,
        key: Union[Key, str],
        touch_ttl: Optional[int] = None,
        lease_policy: Optional[LeasePolicy] = None,
        recache_policy: Optional[RecachePolicy] = None,
        return_cas_token: bool = False,
    ) -> Optional[Value]:
        ...  # pragma: no cover

    def get_typed(
        self,
        key: Union[Key, str],
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Optional[T]:
        ...  # pragma: no cover

    def get_cas_typed(
        self,
        key: Union[Key, str],
        cls: Type[T],
        touch_ttl: Optional[int] = None,
        recache_policy: Optional[RecachePolicy] = None,
        error_on_type_mismatch: bool = False,
    ) -> Tuple[Optional[T], Optional[int]]:
        ...  # pragma: no cover

    def delta(
        self,
        key: Union[Key, str],
        delta: int,
        refresh_ttl: Optional[int] = None,
        no_reply: bool = False,
        cas_token: Optional[int] = None,
    ) -> bool:
        ...  # pragma: no cover

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
        ...  # pragma: no cover

    def delta_and_get(
        self,
        key: Union[Key, str],
        delta: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:
        ...  # pragma: no cover

    def delta_initialize_and_get(
        self,
        key: Union[Key, str],
        delta: int,
        initial_value: int,
        initial_ttl: int,
        refresh_ttl: Optional[int] = None,
        cas_token: Optional[int] = None,
    ) -> Optional[int]:
        ...  # pragma: no cover
