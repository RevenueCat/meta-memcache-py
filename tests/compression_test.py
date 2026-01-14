import threading
from pathlib import Path
from typing import List, Type

import pytest
import zstandard as zstd

from meta_memcache.compression.zstd_manager import (
    BaseZstdManager,
    PooledZstdManager,
    ThreadLocalZstdManager,
)
from meta_memcache.protocol import Key


@pytest.fixture
def default_dictionary() -> zstd.ZstdCompressionDict:
    return zstd.train_dictionary(100 * 1024, [b"default", b"test", b"dictionary"] * 100)


@pytest.fixture
def custom_dictionary() -> zstd.ZstdCompressionDict:
    return zstd.train_dictionary(100 * 1024, [b"custom", b"test", b"dictionary"] * 100)


@pytest.fixture(params=[ThreadLocalZstdManager, PooledZstdManager])
def manager_class(request) -> Type[BaseZstdManager]:
    """Parametrized fixture that returns each manager implementation."""
    return request.param


@pytest.fixture
def manager(manager_class) -> BaseZstdManager:
    """Create a manager instance of the current parametrized type."""
    return manager_class()


class TestBothZstdManagers:
    """Tests that apply to both ThreadLocalZstdManager and PooledZstdManager."""

    def test_instance_isolation(
        self,
        manager_class: Type[BaseZstdManager],
        default_dictionary: zstd.ZstdCompressionDict,
    ) -> None:
        manager1 = manager_class()
        manager2 = manager_class()

        assert manager1 is not manager2

        manager1.set_default_dictionary(default_dictionary.as_bytes())

        key = Key(key="test")
        data = b"test data" * 100

        compressed1 = manager1.compress(data, key.domain)

        with pytest.raises(ValueError, match="Unknown dictionary id"):
            manager2.decompress(compressed1)

    def test_register_dictionary_with_bytes(
        self, manager: BaseZstdManager, custom_dictionary: zstd.ZstdCompressionDict
    ) -> None:
        dict_id = manager.register_dictionary(
            dictionary=custom_dictionary.as_bytes(),
            domains=["users", "posts"],
        )

        assert dict_id == custom_dictionary.dict_id()

    def test_register_dictionary_with_path(
        self,
        manager: BaseZstdManager,
        tmp_path: Path,
        custom_dictionary: zstd.ZstdCompressionDict,
    ) -> None:
        dict_file = tmp_path / "test_dict.zstd"
        dict_file.write_bytes(custom_dictionary.as_bytes())

        dict_id = manager.register_dictionary(
            dictionary=dict_file,
            domains=["users"],
        )

        assert dict_id == custom_dictionary.dict_id()

    def test_domain_based_routing(
        self,
        manager: BaseZstdManager,
        default_dictionary: zstd.ZstdCompressionDict,
        custom_dictionary: zstd.ZstdCompressionDict,
    ) -> None:
        manager.register_dictionary(custom_dictionary.as_bytes(), domains=["users"])
        manager.set_default_dictionary(default_dictionary.as_bytes())

        key_with_domain = Key(key="test", domain="users")
        key_without_domain = Key(key="test")

        data = b"test data" * 100

        compressed_with_domain = manager.compress(data, key_with_domain.domain)
        compressed_without_domain = manager.compress(data, key_without_domain.domain)

        assert compressed_with_domain != compressed_without_domain

        decompressed1 = manager.decompress(compressed_with_domain)
        decompressed2 = manager.decompress(compressed_without_domain)

        assert decompressed1 == data
        assert decompressed2 == data

    def test_default_dictionary_fallback(
        self, manager: BaseZstdManager, default_dictionary: zstd.ZstdCompressionDict
    ) -> None:
        manager.set_default_dictionary(default_dictionary.as_bytes())

        key = Key(key="test")
        data = b"test data" * 100

        compressed = manager.compress(data, key.domain)
        decompressed = manager.decompress(compressed)

        assert decompressed == data

    def test_compress_decompress_round_trip(
        self, manager: BaseZstdManager, default_dictionary: zstd.ZstdCompressionDict
    ) -> None:
        manager.set_default_dictionary(default_dictionary.as_bytes())

        key = Key(key="test")
        original_data = b"Hello, World! " * 1000

        compressed = manager.compress(original_data, key.domain)
        decompressed = manager.decompress(compressed)

        assert decompressed == original_data
        assert len(compressed) < len(original_data)

    def test_compress_without_dictionary(self, manager: BaseZstdManager) -> None:
        key = Key(key="test")
        data = b"test data" * 100

        compressed = manager.compress(data, key.domain)
        decompressed = manager.decompress(compressed)

        assert decompressed == data

    def test_unknown_dictionary_raises_error(
        self,
        manager_class: Type[BaseZstdManager],
        custom_dictionary: zstd.ZstdCompressionDict,
    ) -> None:
        manager1 = manager_class()
        manager2 = manager_class()

        manager1.register_dictionary(custom_dictionary.as_bytes(), domains=["test"])

        key = Key(key="test", domain="test")
        data = b"test data" * 100

        compressed = manager1.compress(data, key.domain)

        with pytest.raises(ValueError, match="Unknown dictionary id"):
            manager2.decompress(compressed)

    def test_get_compressor_unknown_dict_raises_error(
        self, manager: BaseZstdManager
    ) -> None:
        # Try to get compressor with a non-existent dict_id
        with pytest.raises(ValueError, match="Unknown dictionary id"):
            with manager.get_compressor(dict_id=99999):
                pass

    def test_get_decompressor_unknown_dict_raises_error(
        self,
        manager_class: Type[BaseZstdManager],
        custom_dictionary: zstd.ZstdCompressionDict,
    ) -> None:
        manager1 = manager_class()
        manager2 = manager_class()

        manager1.register_dictionary(custom_dictionary.as_bytes(), domains=["test"])

        # Compress data with manager1 using the custom dictionary
        key = Key(key="test", domain="test")
        data = b"test data" * 100
        compressed = manager1.compress(data, key.domain)

        # Extract the dict_id from compressed data
        dict_id = zstd.get_frame_parameters(
            compressed, format=zstd.FORMAT_ZSTD1_MAGICLESS
        ).dict_id

        # Try to get decompressor in manager2 which doesn't have this dictionary
        with pytest.raises(ValueError, match="Unknown dictionary id"):
            with manager2.get_decompressor(dict_id=dict_id):
                pass

    def test_decompress_with_unloaded_dict_raises_error(
        self,
        manager_class: Type[BaseZstdManager],
        custom_dictionary: zstd.ZstdCompressionDict,
    ) -> None:
        manager1 = manager_class()
        manager2 = manager_class()

        manager1.register_dictionary(custom_dictionary.as_bytes(), domains=["test"])

        key = Key(key="test", domain="test")
        data = b"test data" * 100

        # Compress with manager1
        compressed = manager1.compress(data, key.domain)

        # Try to decompress with manager2 which doesn't have the dictionary
        with pytest.raises(ValueError, match="Unknown dictionary id"):
            manager2.decompress(compressed)

    def test_multiple_operations(
        self, manager: BaseZstdManager, default_dictionary: zstd.ZstdCompressionDict
    ) -> None:
        """Test that multiple compress/decompress operations work correctly."""
        manager.set_default_dictionary(default_dictionary.as_bytes())

        key = Key(key="test")
        data = b"test data" * 100

        for _ in range(10):
            compressed = manager.compress(data, key.domain)
            decompressed = manager.decompress(compressed)
            assert decompressed == data

    def test_context_manager_api(
        self, manager: BaseZstdManager, default_dictionary: zstd.ZstdCompressionDict
    ) -> None:
        """Test explicit context manager API for getting compressor/decompressor."""
        dict_id = manager.set_default_dictionary(default_dictionary.as_bytes())

        data = b"test data" * 100

        with manager.get_compressor(dict_id) as compressor:
            compressed = compressor.compress(data)

        with manager.get_decompressor(dict_id) as decompressor:
            decompressed = decompressor.decompress(compressed)

        assert decompressed == data

    def test_thread_safety(
        self,
        manager: BaseZstdManager,
        default_dictionary: zstd.ZstdCompressionDict,
    ) -> None:
        manager.set_default_dictionary(default_dictionary.as_bytes())

        results: List[bytes] = []
        errors: List[Exception] = []

        def compress_decompress_worker(thread_id: int) -> None:
            try:
                key = Key(key=f"test_{thread_id}")
                data = f"thread_{thread_id}_data".encode() * 100

                for _ in range(10):
                    compressed = manager.compress(data, key.domain)
                    decompressed = manager.decompress(compressed)

                assert decompressed == data
                results.append(decompressed)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=compress_decompress_worker, args=(i,))
            for i in range(10)
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0
        assert len(results) == 10
