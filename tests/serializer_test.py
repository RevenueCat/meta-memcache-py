import pickle
import pytest
from meta_memcache.serializer import (
    MixedSerializer,
    ZstdSerializer,
    DictionaryMapping,
    EncodedValue,
)
from meta_memcache.compression import ThreadLocalZstdManager
from meta_memcache.protocol import Key
import zstandard as zstd


ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


@pytest.fixture
def default_dictionary() -> zstd.ZstdCompressionDict:
    return zstd.train_dictionary(100 * 1024, [b"default", b"test", b"dictionary"] * 100)


@pytest.fixture
def custom_dictionary() -> zstd.ZstdCompressionDict:
    return zstd.train_dictionary(100 * 1024, [b"custom", b"test", b"dictionary"] * 100)


@pytest.fixture
def dictionary_mapping(custom_dictionary):
    return [
        DictionaryMapping(
            dictionary=custom_dictionary.as_bytes(), active_domains=["example"]
        )
    ]


@pytest.fixture
def basic_manager() -> ThreadLocalZstdManager:
    """Manager with default configuration, no dictionaries."""
    return ThreadLocalZstdManager(
        compression_level=ZstdSerializer.DEFAULT_COMPRESSION_LEVEL
    )


@pytest.fixture
def manager_with_custom_dict(custom_dictionary) -> ThreadLocalZstdManager:
    """Manager with custom dictionary for 'example' domain."""
    manager = ThreadLocalZstdManager(
        compression_level=ZstdSerializer.DEFAULT_COMPRESSION_LEVEL
    )
    manager.register_dictionary(custom_dictionary.as_bytes(), domains=["example"])
    return manager


@pytest.fixture
def manager_with_both_dicts(
    custom_dictionary, default_dictionary
) -> ThreadLocalZstdManager:
    """Manager with custom dictionary for 'example' domain and default dictionary."""
    manager = ThreadLocalZstdManager(
        compression_level=ZstdSerializer.DEFAULT_COMPRESSION_LEVEL
    )
    manager.register_dictionary(custom_dictionary.as_bytes(), domains=["example"])
    manager.set_default_dictionary(default_dictionary.as_bytes())
    return manager


def get_data_compression_dict(data: bytes) -> int:
    return zstd.get_frame_parameters(data, format=zstd.FORMAT_ZSTD1_MAGICLESS).dict_id


@pytest.mark.parametrize("serializer_class", [ZstdSerializer, MixedSerializer])
def test_serialize_bytes(serializer_class, basic_manager):
    if serializer_class == ZstdSerializer:
        serializer = serializer_class(zstd_manager=basic_manager)
    else:
        serializer = serializer_class()
    data = b"test data"
    key = Key("foo")
    encoded_value = serializer.serialize(key, data)
    assert isinstance(encoded_value, EncodedValue)
    assert encoded_value.encoding_id == serializer.BINARY
    assert encoded_value.data == data
    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


@pytest.mark.parametrize("serializer_class", [ZstdSerializer, MixedSerializer])
def test_serialize_int(serializer_class, basic_manager):
    if serializer_class == ZstdSerializer:
        serializer = serializer_class(zstd_manager=basic_manager)
    else:
        serializer = serializer_class()
    data = 123
    key = Key("foo")
    encoded_value = serializer.serialize(key, data)
    assert isinstance(encoded_value, EncodedValue)
    assert encoded_value.encoding_id == serializer.INT
    assert encoded_value.data == b"123"
    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


@pytest.mark.parametrize("serializer_class", [ZstdSerializer, MixedSerializer])
def test_serialize_string(serializer_class, basic_manager):
    if serializer_class == ZstdSerializer:
        serializer = serializer_class(zstd_manager=basic_manager)
    else:
        serializer = serializer_class()
    data = "test"
    key = Key("foo")
    encoded_value = serializer.serialize(key, data)
    assert isinstance(encoded_value, EncodedValue)
    assert encoded_value.encoding_id == serializer.STR
    assert encoded_value.data == data.encode()
    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


@pytest.mark.parametrize("serializer_class", [ZstdSerializer, MixedSerializer])
def test_serialize_complex(serializer_class, basic_manager):
    if serializer_class == ZstdSerializer:
        serializer = serializer_class(zstd_manager=basic_manager)
    else:
        serializer = serializer_class()
    data = [1, 2, 3]
    key = Key("foo")
    encoded_value = serializer.serialize(key, data)
    assert isinstance(encoded_value, EncodedValue)
    assert encoded_value.encoding_id == serializer.PICKLE
    assert len(encoded_value.data) > 0
    assert pickle.loads(encoded_value.data) == data  # noqa: S301
    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


@pytest.mark.parametrize("serializer_class", [ZstdSerializer, MixedSerializer])
def test_serialize_compress(serializer_class, basic_manager):
    if serializer_class == ZstdSerializer:
        serializer = serializer_class(zstd_manager=basic_manager)
    else:
        serializer = serializer_class()
    data = ["test"] * 100
    key = Key("foo")
    encoded_value = serializer.serialize(key, data)
    assert isinstance(encoded_value, EncodedValue)
    if serializer_class == MixedSerializer:
        assert (
            encoded_value.encoding_id == serializer.PICKLE | serializer.ZLIB_COMPRESSED
        )
    else:
        assert (
            encoded_value.encoding_id == serializer.PICKLE | serializer.ZSTD_COMPRESSED
        )
    assert len(encoded_value.data) < 100
    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


def test_zstd_serializer_understands_zlib(basic_manager):
    mixed_serializer = MixedSerializer()
    data = b"compressed with zlib" * 100
    encoded_value = mixed_serializer.serialize(Key("foo"), data)
    print(MixedSerializer.__dict__)
    assert encoded_value.encoding_id == (
        MixedSerializer.BINARY | MixedSerializer.ZLIB_COMPRESSED
    )

    zstd_serializer = ZstdSerializer(zstd_manager=basic_manager)
    assert (
        zstd_serializer.unserialize(encoded_value.data, encoded_value.encoding_id)
        == data
    )


def test_zstd_decompresses_zstd_when_zstd_is_disabled(basic_manager):
    enabled_serializer = ZstdSerializer(zstd_manager=basic_manager, default_zstd=True)
    data = "compressed with zstd" * 100
    zstd_encoded_value = enabled_serializer.serialize(Key("foo"), data)
    assert zstd_encoded_value.encoding_id == ZstdSerializer.ZSTD_COMPRESSED

    disabled_serializer = ZstdSerializer(zstd_manager=basic_manager, default_zstd=False)
    zlib_encoded_value = disabled_serializer.serialize(Key("foo"), data)
    assert zlib_encoded_value.encoding_id == ZstdSerializer.ZLIB_COMPRESSED

    assert (
        disabled_serializer.unserialize(
            zstd_encoded_value.data, zstd_encoded_value.encoding_id
        )
        == data
    )
    assert (
        disabled_serializer.unserialize(
            zlib_encoded_value.data, zlib_encoded_value.encoding_id
        )
        == data
    )


def test_zstd_default_dict_compression(manager_with_both_dicts, default_dictionary):
    default_dictionary_id = default_dictionary.dict_id()

    serializer = ZstdSerializer(
        zstd_manager=manager_with_both_dicts,
        compression_threshold=101,
        dict_compression_threshold=51,
    )

    data = "test_" * 10

    encoded_value = serializer.serialize(Key("foo"), data)
    assert encoded_value.encoding_id == serializer.STR

    data = "test_" * 11

    encoded_value = serializer.serialize(Key("foo"), data)
    assert encoded_value.encoding_id == serializer.STR | serializer.ZSTD_COMPRESSED
    assert get_data_compression_dict(encoded_value.data) == default_dictionary_id

    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


def test_zstd_no_default_dict_compression(manager_with_custom_dict):
    serializer = ZstdSerializer(
        zstd_manager=manager_with_custom_dict,
        compression_threshold=101,
        dict_compression_threshold=51,
    )

    data = "test_" * 10

    encoded_value = serializer.serialize(Key("foo"), data)
    assert encoded_value.encoding_id == serializer.STR

    data = "test_" * 11

    encoded_value = serializer.serialize(Key("foo"), data)
    assert encoded_value.encoding_id == serializer.STR

    data = "test_" * 21
    encoded_value = serializer.serialize(Key("foo"), data)
    assert encoded_value.encoding_id == serializer.STR | serializer.ZSTD_COMPRESSED
    assert get_data_compression_dict(encoded_value.data) == 0

    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


def test_zstd_custom_dict_compression(manager_with_both_dicts, custom_dictionary):
    custom_dictionary_id = custom_dictionary.dict_id()

    serializer = ZstdSerializer(
        zstd_manager=manager_with_both_dicts,
        compression_threshold=101,
        dict_compression_threshold=51,
    )

    data = "test_" * 10

    encoded_value = serializer.serialize(Key("foo", domain="example"), data)
    assert encoded_value.encoding_id == serializer.STR

    data = "test_" * 11

    encoded_value = serializer.serialize(Key("foo", domain="example"), data)
    assert encoded_value.encoding_id == serializer.STR | serializer.ZSTD_COMPRESSED
    assert get_data_compression_dict(encoded_value.data) == custom_dictionary_id

    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


def test_zstd_format_parameter():
    # Test default format (FORMAT_ZSTD1_MAGICLESS)
    default_manager = ThreadLocalZstdManager()
    assert default_manager._zstd_format == zstd.FORMAT_ZSTD1_MAGICLESS
    serializer_default = ZstdSerializer(zstd_manager=default_manager)

    data = "test" * 100
    key = Key("foo")
    encoded_value = serializer_default.serialize(key, data)
    assert encoded_value.encoding_id == ZstdSerializer.ZSTD_COMPRESSED
    assert not encoded_value.data.startswith(ZSTD_MAGIC)
    # Should be able to unserialize
    assert (
        serializer_default.unserialize(encoded_value.data, encoded_value.encoding_id)
        == data
    )
    # Adding the magic bytes should be able to decompress
    assert zstd.decompress(ZSTD_MAGIC + encoded_value.data).decode() == data

    # Test with FORMAT_ZSTD1
    manager_with_magic = ThreadLocalZstdManager(zstd_format=zstd.FORMAT_ZSTD1)
    serializer_with_magic = ZstdSerializer(zstd_manager=manager_with_magic)
    encoded_value = serializer_with_magic.serialize(key, data)
    assert encoded_value.encoding_id == ZstdSerializer.ZSTD_COMPRESSED
    assert encoded_value.data.startswith(ZSTD_MAGIC)
    # Should be able to unserialize
    assert (
        serializer_with_magic.unserialize(encoded_value.data, encoded_value.encoding_id)
        == data
    )
