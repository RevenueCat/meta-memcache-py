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

    def delete(
        self,
        key: Union[Key, str],
        cas_token: Optional[int] = None,
        no_reply: bool = False,
        stale_policy: Optional[StalePolicy] = None,
    ) -> bool:
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
