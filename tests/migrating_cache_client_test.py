from unittest.mock import Mock

import pytest
from meta_memcache import CacheClient, IntFlag, Key, SetMode, Value, WriteFailureEvent
from meta_memcache.extras.migrating_cache_client import (
    MigratingCacheClient,
    MigrationMode,
)


@pytest.fixture
def origin_client() -> Mock:
    return Mock(spec=CacheClient)


@pytest.fixture
def destination_client() -> Mock:
    return Mock(spec=CacheClient)


@pytest.fixture
def time(monkeypatch) -> Mock:
    time_mock = Mock()
    monkeypatch.setattr("meta_memcache.extras.migrating_cache_client.time", time_mock)
    return time_mock


@pytest.fixture
def random(monkeypatch) -> Mock:
    random_mock = Mock()
    monkeypatch.setattr(
        "meta_memcache.extras.migrating_cache_client.random", random_mock
    )
    return random_mock


@pytest.fixture
def migration_client_origin_only(
    origin_client: CacheClient, destination_client: CacheClient
) -> MigratingCacheClient:
    return MigratingCacheClient(
        origin_client, destination_client, MigrationMode.ONLY_ORIGIN
    )


@pytest.fixture
def migration_client_destination_only(
    origin_client: CacheClient, destination_client: CacheClient
) -> MigratingCacheClient:
    return MigratingCacheClient(
        origin_client, destination_client, MigrationMode.ONLY_DESTINATION
    )


@pytest.fixture
def migration_client(
    origin_client: CacheClient, destination_client: CacheClient
) -> MigratingCacheClient:
    return MigratingCacheClient(
        origin_client,
        destination_client,
        migration_mode_config={
            MigrationMode.ONLY_ORIGIN: 10,
            MigrationMode.POPULATE_WRITES: 20,
            MigrationMode.POPULATE_WRITES_AND_READS_1PCT: 30,
            MigrationMode.POPULATE_WRITES_AND_READS_10PCT: 40,
            MigrationMode.USE_DESTINATION_UPDATE_ORIGIN: 50,
            MigrationMode.ONLY_DESTINATION: 60,
        },
    )


def _set_cache_client_mock_get_return_values(client: Mock, ttl: int = 10) -> None:
    client.meta_get.return_value = Value(
        size=3,
        value="bar",
        ttl=ttl,
    )
    client.meta_multiget.return_value = {
        Key(key="foo", routing_key=None, is_unicode=False): Value(
            size=3,
            value="bar",
            ttl=ttl,
        )
    }


def test_on_write_failure(
    migration_client_origin_only: MigratingCacheClient,
    origin_client: CacheClient,
    destination_client: CacheClient,
) -> None:
    errors = []

    def track_errors(key):
        errors.append(key)

    assert isinstance(migration_client_origin_only.on_write_failure, WriteFailureEvent)
    migration_client_origin_only.on_write_failure += track_errors
    origin_client.on_write_failure("key_on_origin")
    destination_client.on_write_failure("key_on_destination")
    assert errors == ["key_on_origin", "key_on_destination"]


def test_migration_mode(time: Mock, migration_client: MigratingCacheClient) -> None:
    time.time.return_value = 0
    assert migration_client.get_migration_mode() == MigrationMode.ONLY_ORIGIN
    time.time.return_value = 10
    assert migration_client.get_migration_mode() == MigrationMode.ONLY_ORIGIN
    time.time.return_value = 20
    assert migration_client.get_migration_mode() == MigrationMode.POPULATE_WRITES
    time.time.return_value = 30
    assert (
        migration_client.get_migration_mode()
        == MigrationMode.POPULATE_WRITES_AND_READS_1PCT
    )
    time.time.return_value = 40
    assert (
        migration_client.get_migration_mode()
        == MigrationMode.POPULATE_WRITES_AND_READS_10PCT
    )
    time.time.return_value = 50
    assert (
        migration_client.get_migration_mode()
        == MigrationMode.USE_DESTINATION_UPDATE_ORIGIN
    )
    time.time.return_value = 60
    assert migration_client.get_migration_mode() == MigrationMode.ONLY_DESTINATION


def test_migration_mode_origin_only(
    migration_client_origin_only: MigratingCacheClient,
    origin_client: CacheClient,
    destination_client: CacheClient,
):
    _set_cache_client_mock_get_return_values(origin_client)

    # Gets
    migration_client_origin_only.get(key="foo")
    origin_client.meta_get.assert_called_once()
    destination_client.meta_get.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Multi-gets
    migration_client_origin_only.multi_get(keys=["foo"])
    origin_client.meta_multiget.assert_called_once()
    destination_client.meta_multiget.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Sets
    migration_client_origin_only.set(key="foo", value="bar", ttl=10)
    origin_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )
    destination_client.meta_set.assert_not_called()

    # Deletes
    migration_client_origin_only.delete(key="foo")
    origin_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )
    destination_client.meta_delete.assert_not_called()

    # Arithmetic
    migration_client_origin_only.delta(key="foo", delta=1)
    origin_client.meta_arithmetic.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={IntFlag.MA_DELTA_VALUE: 1},
        token_flags={},
    )
    destination_client.meta_arithmetic.assert_not_called()

    # Touch
    migration_client_origin_only.touch(key="foo", ttl=10)
    origin_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)
    destination_client.touch.assert_not_called()


def test_migration_mode_destination_only(
    migration_client_destination_only: MigratingCacheClient,
    origin_client: CacheClient,
    destination_client: CacheClient,
):
    _set_cache_client_mock_get_return_values(destination_client)

    # Gets
    migration_client_destination_only.get(key="foo")
    origin_client.meta_get.assert_not_called()
    destination_client.meta_get.assert_called_once()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Multi-get
    migration_client_destination_only.multi_get(keys=["foo"])
    origin_client.meta_multiget.assert_not_called()
    destination_client.meta_multiget.assert_called_once()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Sets
    migration_client_destination_only.set(key="foo", value="bar", ttl=10)
    origin_client.meta_set.assert_not_called()
    destination_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )

    # Deletes
    migration_client_destination_only.delete(key="foo")
    origin_client.meta_delete.assert_not_called()
    destination_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )

    # Arithmetic
    migration_client_destination_only.delta(key="foo", delta=1)
    origin_client.meta_arithmetic.assert_not_called()
    destination_client.meta_arithmetic.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={IntFlag.MA_DELTA_VALUE: 1},
        token_flags={},
    )

    # Touch
    migration_client_destination_only.touch(key="foo", ttl=10)
    origin_client.touch.assert_not_called()
    destination_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)


def test_migration_mode_populate_writes(
    time: Mock,
    migration_client: MigratingCacheClient,
    origin_client: CacheClient,
    destination_client: CacheClient,
) -> None:
    time.time.return_value = 20
    assert migration_client.get_migration_mode() == MigrationMode.POPULATE_WRITES

    _set_cache_client_mock_get_return_values(origin_client)

    # Gets
    migration_client.get(key="foo")
    origin_client.meta_get.assert_called_once()
    destination_client.meta_get.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Multi-get
    migration_client.multi_get(keys=["foo"])
    origin_client.meta_multiget.assert_called_once()
    destination_client.meta_multiget.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Sets (both receive writes)
    migration_client.set(key="foo", value="bar", ttl=10)
    origin_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )
    destination_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )

    # Deletes (both receive writes)
    migration_client.delete(key="foo")
    origin_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )
    destination_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )

    # Arithmetic
    migration_client.delta(key="foo", delta=1)
    origin_client.meta_arithmetic.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={IntFlag.MA_DELTA_VALUE: 1},
        token_flags={},
    )
    destination_client.meta_arithmetic.assert_not_called()

    # Touch
    migration_client.touch(key="foo", ttl=10)
    origin_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)
    destination_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)


def test_migration_mode_populate_reads_handles_non_expiring_keys(
    time: Mock,
    random: Mock,
    migration_client: MigratingCacheClient,
    origin_client: CacheClient,
    destination_client: CacheClient,
) -> None:
    time.time.return_value = 30
    assert (
        migration_client.get_migration_mode()
        == MigrationMode.POPULATE_WRITES_AND_READS_1PCT
    )

    # Non expiring items in cache return ttl of -1, but
    # we need to populate them as ttl=0
    _set_cache_client_mock_get_return_values(origin_client, ttl=-1)
    # Random says reads should be populated
    random.getrandbits.return_value = 1000

    # Gets
    migration_client.get(key="foo")
    origin_client.meta_get.assert_called_once()
    destination_client.meta_get.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=0,
        no_reply=True,
        set_mode=SetMode.ADD,
    )
    destination_client.set.reset_mock()

    # Multi-get
    migration_client.multi_get(keys=["foo"])
    origin_client.meta_multiget.assert_called_once()
    destination_client.meta_multiget.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=0,
        no_reply=True,
        set_mode=SetMode.ADD,
    )


def test_migration_mode_populate_writes_and_reads_1pct(
    time: Mock,
    random: Mock,
    migration_client: MigratingCacheClient,
    origin_client: CacheClient,
    destination_client: CacheClient,
) -> None:
    time.time.return_value = 30
    assert (
        migration_client.get_migration_mode()
        == MigrationMode.POPULATE_WRITES_AND_READS_1PCT
    )

    _set_cache_client_mock_get_return_values(origin_client)

    # Random says reads should NOT be populated
    random.getrandbits.return_value = 1

    # Gets (not populated)
    migration_client.get(key="foo")
    origin_client.meta_get.assert_called_once()
    destination_client.meta_get.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Multi-get
    migration_client.multi_get(keys=["foo"])
    origin_client.meta_multiget.assert_called_once()
    destination_client.meta_multiget.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    origin_client.meta_get.reset_mock()
    origin_client.meta_multiget.reset_mock()

    # Random says reads should be populated
    random.getrandbits.return_value = 1000

    # Gets
    migration_client.get(key="foo")
    origin_client.meta_get.assert_called_once()
    destination_client.meta_get.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        no_reply=True,
        set_mode=SetMode.ADD,
    )
    destination_client.set.reset_mock()

    # Multi-get
    migration_client.multi_get(keys=["foo"])
    origin_client.meta_multiget.assert_called_once()
    destination_client.meta_multiget.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        no_reply=True,
        set_mode=SetMode.ADD,
    )

    # Sets (both receive writes)
    migration_client.set(key="foo", value="bar", ttl=10)
    origin_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )
    destination_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )

    # Deletes (both receive writes)
    migration_client.delete(key="foo")
    origin_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )
    destination_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )

    # Arithmetic
    migration_client.delta(key="foo", delta=1)
    origin_client.meta_arithmetic.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={IntFlag.MA_DELTA_VALUE: 1},
        token_flags={},
    )
    destination_client.meta_arithmetic.assert_not_called()

    # Touch
    migration_client.touch(key="foo", ttl=10)
    origin_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)
    destination_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)


def test_migration_mode_populate_writes_and_reads_10pct(
    time: Mock,
    random: Mock,
    migration_client: MigratingCacheClient,
    origin_client: CacheClient,
    destination_client: CacheClient,
) -> None:
    time.time.return_value = 40
    assert (
        migration_client.get_migration_mode()
        == MigrationMode.POPULATE_WRITES_AND_READS_10PCT
    )

    _set_cache_client_mock_get_return_values(origin_client)

    # Random says reads should NOT be populated
    random.getrandbits.return_value = 10

    # Gets (not populated)
    migration_client.get(key="foo")
    origin_client.meta_get.assert_called_once()
    destination_client.meta_get.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Multi-get
    migration_client.multi_get(keys=["foo"])
    origin_client.meta_multiget.assert_called_once()
    destination_client.meta_multiget.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    origin_client.meta_get.reset_mock()
    origin_client.meta_multiget.reset_mock()

    # Random says reads should be populated
    random.getrandbits.return_value = 1009

    # Gets
    migration_client.get(key="foo")
    origin_client.meta_get.assert_called_once()
    destination_client.meta_get.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        no_reply=True,
        set_mode=SetMode.ADD,
    )
    destination_client.set.reset_mock()

    # Multi-get
    migration_client.multi_get(keys=["foo"])
    origin_client.meta_multiget.assert_called_once()
    destination_client.meta_multiget.assert_not_called()
    origin_client.set.assert_not_called()
    destination_client.set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        no_reply=True,
        set_mode=SetMode.ADD,
    )

    # Sets (both receive writes)
    migration_client.set(key="foo", value="bar", ttl=10)
    origin_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )
    destination_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )

    # Deletes (both receive writes)
    migration_client.delete(key="foo")
    origin_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )
    destination_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )

    # Arithmetic
    migration_client.delta(key="foo", delta=1)
    origin_client.meta_arithmetic.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={IntFlag.MA_DELTA_VALUE: 1},
        token_flags={},
    )
    destination_client.meta_arithmetic.assert_not_called()

    # Touch
    migration_client.touch(key="foo", ttl=10)
    origin_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)
    destination_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)


def test_migration_mode_use_destination_update_origin(
    time: Mock,
    migration_client: MigratingCacheClient,
    origin_client: CacheClient,
    destination_client: CacheClient,
) -> None:
    time.time.return_value = 50
    assert (
        migration_client.get_migration_mode()
        == MigrationMode.USE_DESTINATION_UPDATE_ORIGIN
    )

    _set_cache_client_mock_get_return_values(destination_client)

    # Gets
    migration_client.get(key="foo")
    origin_client.meta_get.assert_not_called()
    destination_client.meta_get.assert_called_once()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Multi-get
    migration_client.multi_get(keys=["foo"])
    origin_client.meta_multiget.assert_not_called()
    destination_client.meta_multiget.assert_called_once()
    origin_client.set.assert_not_called()
    destination_client.set.assert_not_called()

    # Sets (both receive writes)
    migration_client.set(key="foo", value="bar", ttl=10)
    origin_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )
    destination_client.meta_set.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        value="bar",
        ttl=10,
        flags=set(),
        int_flags={IntFlag.CACHE_TTL: 10},
        token_flags=None,
    )

    # Deletes (both receive writes)
    migration_client.delete(key="foo")
    origin_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )
    destination_client.meta_delete.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={},
        token_flags=None,
    )

    # Arithmetic
    migration_client.delta(key="foo", delta=1)
    origin_client.meta_arithmetic.assert_not_called()
    destination_client.meta_arithmetic.assert_called_once_with(
        key=Key(key="foo", routing_key=None, is_unicode=False),
        flags=set(),
        int_flags={IntFlag.MA_DELTA_VALUE: 1},
        token_flags={},
    )

    # Touch
    migration_client.touch(key="foo", ttl=10)
    origin_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)
    destination_client.touch.assert_called_once_with(key="foo", ttl=10, no_reply=False)
