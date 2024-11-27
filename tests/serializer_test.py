import pickle
import pytest
from meta_memcache.serializer import (
    MixedSerializer,
    ZstdSerializer,
    DictionaryMapping,
    EncodedValue,
    Key,
)
import zstandard as zstd


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


def get_compression_dict(compressor: zstd.ZstdCompressor) -> int:
    return get_data_compression_dict(compressor.compress(b"test"))


def get_data_compression_dict(data: bytes) -> int:
    return zstd.get_frame_parameters(data, format=zstd.FORMAT_ZSTD1_MAGICLESS).dict_id


def test_zstd_serializer_initialization(
    dictionary_mapping, default_dictionary, custom_dictionary
):
    default_dictionary_id = default_dictionary.dict_id()
    custom_dictionary_id = custom_dictionary.dict_id()

    # Test with dictionary mappings, default to zstd without dictionary
    serializer = ZstdSerializer(dictionary_mappings=dictionary_mapping)
    assert serializer._pickle_protocol == ZstdSerializer.DEFAULT_PICKLE_PROTOCOL
    assert serializer._compression_level == ZstdSerializer.DEFAULT_COMPRESSION_LEVEL
    assert (
        serializer._default_compression_threshold
        == ZstdSerializer.DEFAULT_COMPRESSION_THRESHOLD
    )
    assert (
        serializer._dict_compression_threshold
        == ZstdSerializer.DEFAULT_DICT_COMPRESSION_THRESHOLD
    )
    assert isinstance(serializer._default_zstd_compressor, zstd.ZstdCompressor)
    assert get_compression_dict(serializer._default_zstd_compressor) == 0
    assert set(serializer._zstd_compressors.keys()) == {custom_dictionary_id}
    assert set(serializer._zstd_decompressors.keys()) == {0, custom_dictionary_id}
    assert serializer._domain_to_dict_id == {"example": custom_dictionary_id}

    # Test with dictionary mappings, default to zstd with default_dictionary
    serializer = ZstdSerializer(
        dictionary_mappings=dictionary_mapping,
        default_dictionary=default_dictionary.as_bytes(),
    )
    assert serializer._pickle_protocol == ZstdSerializer.DEFAULT_PICKLE_PROTOCOL
    assert serializer._compression_level == ZstdSerializer.DEFAULT_COMPRESSION_LEVEL
    assert (
        serializer._default_compression_threshold
        == ZstdSerializer.DEFAULT_DICT_COMPRESSION_THRESHOLD
    )
    assert (
        serializer._dict_compression_threshold
        == ZstdSerializer.DEFAULT_DICT_COMPRESSION_THRESHOLD
    )
    assert isinstance(serializer._default_zstd_compressor, zstd.ZstdCompressor)
    assert (
        get_compression_dict(serializer._default_zstd_compressor)
        == default_dictionary_id
    )
    assert set(serializer._zstd_compressors.keys()) == {
        custom_dictionary_id,
        default_dictionary_id,
    }
    assert set(serializer._zstd_decompressors.keys()) == {
        0,
        custom_dictionary_id,
        default_dictionary_id,
    }
    assert serializer._domain_to_dict_id == {"example": custom_dictionary_id}

    # Test without dictionary mapping and default to zlib
    serializer = ZstdSerializer(
        default_zstd=False,
    )
    assert serializer._pickle_protocol == ZstdSerializer.DEFAULT_PICKLE_PROTOCOL
    assert serializer._compression_level == ZstdSerializer.DEFAULT_COMPRESSION_LEVEL
    assert (
        serializer._default_compression_threshold
        == ZstdSerializer.DEFAULT_COMPRESSION_THRESHOLD
    )
    assert (
        serializer._dict_compression_threshold
        == ZstdSerializer.DEFAULT_DICT_COMPRESSION_THRESHOLD
    )
    assert serializer._default_zstd_compressor is None
    assert not serializer._zstd_compressors
    assert set(serializer._zstd_decompressors.keys()) == {0}
    assert not serializer._domain_to_dict_id


@pytest.mark.parametrize("serializer_class", [ZstdSerializer, MixedSerializer])
def test_serialize_bytes(serializer_class):
    serializer = serializer_class()
    data = b"test data"
    key = Key("foo")
    encoded_value = serializer.serialize(key, data)
    assert isinstance(encoded_value, EncodedValue)
    assert encoded_value.encoding_id == serializer.BINARY
    assert encoded_value.data == data
    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


@pytest.mark.parametrize("serializer_class", [ZstdSerializer, MixedSerializer])
def test_serialize_int(serializer_class):
    serializer = serializer_class()
    data = 123
    key = Key("foo")
    encoded_value = serializer.serialize(key, data)
    assert isinstance(encoded_value, EncodedValue)
    assert encoded_value.encoding_id == serializer.INT
    assert encoded_value.data == b"123"
    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


@pytest.mark.parametrize("serializer_class", [ZstdSerializer, MixedSerializer])
def test_serialize_string(serializer_class):
    serializer = serializer_class()
    data = "test"
    key = Key("foo")
    encoded_value = serializer.serialize(key, data)
    assert isinstance(encoded_value, EncodedValue)
    assert encoded_value.encoding_id == serializer.STR
    assert encoded_value.data == data.encode()
    assert serializer.unserialize(encoded_value.data, encoded_value.encoding_id) == data


@pytest.mark.parametrize("serializer_class", [ZstdSerializer, MixedSerializer])
def test_serialize_complex(serializer_class):
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
def test_serialize_compress(serializer_class):
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


def test_zstd_serializer_understands_zlib():
    mixed_serializer = MixedSerializer()
    data = b"compressed with zlib" * 100
    encoded_value = mixed_serializer.serialize(Key("foo"), data)
    print(MixedSerializer.__dict__)
    assert encoded_value.encoding_id == (
        MixedSerializer.BINARY | MixedSerializer.ZLIB_COMPRESSED
    )

    zstd_serializer = ZstdSerializer()
    assert (
        zstd_serializer.unserialize(encoded_value.data, encoded_value.encoding_id)
        == data
    )


def test_zstd_default_dict_compression(dictionary_mapping, default_dictionary):
    default_dictionary_id = default_dictionary.dict_id()
    serializer = ZstdSerializer(
        dictionary_mappings=dictionary_mapping,
        default_dictionary=default_dictionary.as_bytes(),
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


def test_zstd_no_default_dict_compression(dictionary_mapping):
    serializer = ZstdSerializer(
        dictionary_mappings=dictionary_mapping,
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


def test_zstd_custom_dict_compression(
    dictionary_mapping, default_dictionary, custom_dictionary
):
    custom_dictionary_id = custom_dictionary.dict_id()
    serializer = ZstdSerializer(
        dictionary_mappings=dictionary_mapping,
        default_dictionary=default_dictionary.as_bytes(),
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
