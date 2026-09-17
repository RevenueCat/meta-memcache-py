"""Failover behavior of the GutterRouter.

The router spots failures by the marker responses the executor returns
(MISS_DUE_TO_ERROR / NOT_STORED_DUE_TO_ERROR) rather than by catching
MemcacheServerError, so it never has to override the caller's
raise_on_server_error on the regular pool.
"""

from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from meta_memcache.configuration import ServerAddress
from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.connection.providers import HostConnectionPoolProvider
from meta_memcache.interfaces.executor import Executor
from meta_memcache.interfaces.router import FailureHandling
from meta_memcache.protocol import (
    MISS_DUE_TO_ERROR,
    NOT_STORED_DUE_TO_ERROR,
    Key,
    MaybeValue,
    MemcacheResponse,
    MetaCommand,
    RequestFlags,
    ResponseFlags,
    Success,
    ValueContainer,
)
from meta_memcache.routers.gutter import GutterRouter

REGULAR = ServerAddress("regular", 11211)
GUTTER = ServerAddress("gutter", 11211)


@pytest.fixture
def executor(mocker: MockerFixture) -> MagicMock:
    return mocker.MagicMock(spec=Executor)


@pytest.fixture
def pools(mocker: MockerFixture) -> Tuple[ConnectionPool, ConnectionPool]:
    return (
        mocker.MagicMock(spec=ConnectionPool),
        mocker.MagicMock(spec=ConnectionPool),
    )


@pytest.fixture
def router(
    executor: MagicMock, pools: Tuple[ConnectionPool, ConnectionPool]
) -> GutterRouter:
    regular_pool, gutter_pool = pools
    return GutterRouter(
        pool_provider=HostConnectionPoolProvider(REGULAR, regular_pool),
        gutter_pool_provider=HostConnectionPoolProvider(GUTTER, gutter_pool),
        gutter_ttl=60,
        executor=executor,
    )


def test_regular_pool_is_never_asked_to_raise(
    router: GutterRouter, executor: MagicMock
) -> None:
    """The caller's raise_on_server_error is no longer hijacked to force a raise."""
    executor.exec_on_pool.return_value = MISS_DUE_TO_ERROR

    router.exec(
        command=MetaCommand.META_GET,
        key=Key("foo"),
        failure_handling=FailureHandling(raise_on_server_error=True),
    )

    regular_call, gutter_call = executor.exec_on_pool.call_args_list
    assert regular_call.kwargs["raise_on_server_error"] is False
    # ... but the gutter pool still honors it, so a double failure raises
    assert gutter_call.kwargs["raise_on_server_error"] is True


@pytest.mark.parametrize(
    "command,failure",
    [
        (MetaCommand.META_GET, MISS_DUE_TO_ERROR),
        (MetaCommand.META_SET, NOT_STORED_DUE_TO_ERROR),
        (MetaCommand.META_DELETE, NOT_STORED_DUE_TO_ERROR),
        (MetaCommand.META_ARITHMETIC, NOT_STORED_DUE_TO_ERROR),
    ],
)
def test_marker_response_fails_over_to_the_gutter_pool(
    router: GutterRouter,
    executor: MagicMock,
    pools: Tuple[ConnectionPool, ConnectionPool],
    command: MetaCommand,
    failure: MemcacheResponse,
) -> None:
    regular_pool, gutter_pool = pools
    success = Success(flags=ResponseFlags())
    executor.exec_on_pool.side_effect = [failure, success]

    result = router.exec(
        command=command,
        key=Key("foo"),
        value=None,
        flags=RequestFlags(cache_ttl=1000),
    )
    assert result is success

    regular_call, gutter_call = executor.exec_on_pool.call_args_list
    assert regular_call.kwargs["pool"] is regular_pool
    assert gutter_call.kwargs["pool"] is gutter_pool
    # The gutter caps the TTL so the entries expire soon
    assert gutter_call.kwargs["flags"].cache_ttl == 60
    # ... and never tracks write failures, it is limited-TTL already
    assert gutter_call.kwargs["track_write_failures"] is False


def test_ordinary_negative_responses_do_not_fail_over(
    router: GutterRouter, executor: MagicMock
) -> None:
    """A real miss/not-stored is the server's answer, not a reason to fail over."""
    from meta_memcache.protocol import Miss, NotStored

    for response in (Miss(), NotStored()):
        executor.reset_mock()
        executor.exec_on_pool.return_value = response
        assert router.exec(command=MetaCommand.META_GET, key=Key("foo")) is response
        executor.exec_on_pool.assert_called_once()


def test_multi_only_retries_the_keys_that_actually_failed(
    router: GutterRouter,
    executor: MagicMock,
    pools: Tuple[ConnectionPool, ConnectionPool],
) -> None:
    """
    Writes are pipelined, so a batch can break part-way through and come back
    with real responses for the keys the server did answer. Those must be kept
    instead of being re-sent to the gutter.
    """
    regular_pool, gutter_pool = pools
    stored = Success(flags=ResponseFlags())
    gutter_stored = Success(flags=ResponseFlags(fetched=True))

    def exec_multi_on_pool(
        pool: ConnectionPool,
        key_values: List[Tuple[Key, MaybeValue]],
        **kwargs: object,
    ) -> Dict[Key, MemcacheResponse]:
        if pool is gutter_pool:
            return {key: gutter_stored for key, _ in key_values}
        # "a" made it onto the wire before the connection broke
        return {
            key: stored if key == Key("a") else NOT_STORED_DUE_TO_ERROR
            for key, _ in key_values
        }

    executor.exec_multi_on_pool.side_effect = exec_multi_on_pool

    keys = [Key("a"), Key("b"), Key("c")]
    results = router.exec_multi(
        command=MetaCommand.META_SET,
        keys=keys,
        values=[ValueContainer(1), ValueContainer(2), ValueContainer(3)],
        flags=RequestFlags(cache_ttl=1000),
    )

    assert results == {
        Key("a"): stored,
        Key("b"): gutter_stored,
        Key("c"): gutter_stored,
    }
    gutter_call = executor.exec_multi_on_pool.call_args_list[1]
    assert gutter_call.kwargs["pool"] is gutter_pool
    assert [key for key, _ in gutter_call.kwargs["key_values"]] == [Key("b"), Key("c")]
    # The values travel along with the keys that get retried
    assert [value.value for _, value in gutter_call.kwargs["key_values"] if value] == [
        2,
        3,
    ]
    assert gutter_call.kwargs["flags"].cache_ttl == 60


def test_multi_does_not_touch_the_gutter_when_nothing_failed(
    router: GutterRouter, executor: MagicMock
) -> None:
    stored = Success(flags=ResponseFlags())
    executor.exec_multi_on_pool.return_value = {Key("a"): stored, Key("b"): stored}

    results = router.exec_multi(command=MetaCommand.META_GET, keys=[Key("a"), Key("b")])

    assert results == {Key("a"): stored, Key("b"): stored}
    executor.exec_multi_on_pool.assert_called_once()
    assert (
        executor.exec_multi_on_pool.call_args.kwargs["raise_on_server_error"] is False
    )


def test_multi_without_values_fails_over_keys_only(
    router: GutterRouter,
    executor: MagicMock,
    pools: Tuple[ConnectionPool, ConnectionPool],
) -> None:
    regular_pool, gutter_pool = pools
    value = Success(flags=ResponseFlags())

    def exec_multi_on_pool(
        pool: ConnectionPool,
        key_values: List[Tuple[Key, MaybeValue]],
        flags: Optional[RequestFlags],
        **kwargs: object,
    ) -> Dict[Key, MemcacheResponse]:
        if pool is gutter_pool:
            return {key: value for key, _ in key_values}
        return {key: MISS_DUE_TO_ERROR for key, _ in key_values}

    executor.exec_multi_on_pool.side_effect = exec_multi_on_pool

    results = router.exec_multi(command=MetaCommand.META_GET, keys=[Key("a")])

    assert results == {Key("a"): value}
    gutter_call = executor.exec_multi_on_pool.call_args_list[1]
    assert gutter_call.kwargs["key_values"] == [(Key("a"), None)]
