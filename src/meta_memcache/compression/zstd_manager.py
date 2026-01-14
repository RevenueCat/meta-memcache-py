import threading
from abc import ABC, abstractmethod
from collections import deque
from contextlib import contextmanager
from pathlib import Path
from typing import Deque, Generator, ByteString

import zstandard as zstd


class ZstdDictionaries:
    def __init__(self) -> None:
        self._dicts: dict[int, zstd.ZstdCompressionDict] = {}
        self._default_dict_id: int | None = None

    def register_dictionary(
        self,
        dictionary: bytes | Path,
        compression_params: zstd.ZstdCompressionParameters,
    ) -> int:
        if isinstance(dictionary, Path):
            dict_bytes = dictionary.read_bytes()
        else:
            dict_bytes = dictionary

        zstd_dict = zstd.ZstdCompressionDict(dict_bytes)
        dict_id = zstd_dict.dict_id()

        if dict_id not in self._dicts:
            zstd_dict.precompute_compress(compression_params=compression_params)
            self._dicts[dict_id] = zstd_dict

        return dict_id

    def get_dictionary(self, dict_id: int) -> zstd.ZstdCompressionDict:
        dict_data = self._dicts.get(dict_id)
        if dict_data is None:
            raise ValueError(f"Unknown dictionary id: {dict_id}")
        return dict_data


class ZstdThreadCache:
    """Thread-local cache for ZstdCompressor/ZstdDecompressor instances."""

    __slots__ = ("_compressors", "_decompressors")

    def __init__(self) -> None:
        self._compressors: dict[int, zstd.ZstdCompressor] = {}
        self._decompressors: dict[int, zstd.ZstdDecompressor] = {}

    def set_compressor(
        self, dict_id: int | None, compressor: zstd.ZstdCompressor
    ) -> None:
        if dict_id is None:
            dict_id = 0
        self._compressors[dict_id] = compressor

    def set_decompressor(
        self, dict_id: int | None, decompressor: zstd.ZstdDecompressor
    ) -> None:
        if dict_id is None:
            dict_id = 0
        self._decompressors[dict_id] = decompressor

    def get_compressor(self, dict_id: int | None) -> zstd.ZstdCompressor | None:
        if dict_id is None:
            dict_id = 0
        return self._compressors.get(dict_id)

    def get_decompressor(self, dict_id: int | None) -> zstd.ZstdDecompressor | None:
        if dict_id is None:
            dict_id = 0
        return self._decompressors.get(dict_id)


class BaseZstdManager(ABC):
    """
    Abstract base class for ZstdCompressor/ZstdDecompressor managers.

    Provides shared logic for dictionary management, domain-based routing,
    """

    def __init__(
        self,
        compression_level: int = 9,
        zstd_format: int = zstd.FORMAT_ZSTD1_MAGICLESS,
    ) -> None:
        self._compression_level = compression_level
        self._zstd_format = zstd_format

        self._dictionaries = ZstdDictionaries()
        self._domain_to_dict_id: dict[str, int] = {}
        self._default_dict_id: int | None = None

        self._compression_params = zstd.ZstdCompressionParameters.from_level(
            compression_level,
            format=zstd_format,
            write_content_size=True,
            write_checksum=False,
            write_dict_id=True,
        )

    def register_dictionary(
        self,
        dictionary: bytes | Path,
        domains: list[str],
    ) -> int:
        """Register a dictionary for specific domains."""
        dict_id = self._dictionaries.register_dictionary(
            dictionary=dictionary,
            compression_params=self._compression_params,
        )

        for domain in domains:
            self._domain_to_dict_id[domain] = dict_id

        return dict_id

    def set_default_dictionary(self, dictionary: bytes | Path) -> int:
        """Set the default dictionary for keys without a domain."""
        dict_id = self._dictionaries.register_dictionary(
            dictionary=dictionary,
            compression_params=self._compression_params,
        )

        self._default_dict_id = dict_id
        return dict_id

    def select_dict_id(self, domain: str | None) -> int | None:
        """Select appropriate dictionary ID based on domain."""
        dict_id = self._default_dict_id
        if domain is not None and (
            domain_dict_id := self._domain_to_dict_id.get(domain)
        ):
            dict_id = domain_dict_id
        return dict_id

    @abstractmethod
    @contextmanager
    def get_compressor(
        self, dict_id: int | None
    ) -> Generator[zstd.ZstdCompressor, None, None]:
        pass

    @abstractmethod
    @contextmanager
    def get_decompressor(
        self, dict_id: int | None
    ) -> Generator[zstd.ZstdDecompressor, None, None]:
        pass

    def compress(self, data: ByteString, domain: str | None) -> bytes:
        """Compress data using the appropriate dictionary based on domain."""
        dict_id = self.select_dict_id(domain)
        with self.get_compressor(dict_id) as compressor:
            return compressor.compress(data)

    def decompress(self, data: ByteString) -> bytes:
        """Decompress data, automatically detecting the dictionary used."""
        dict_id = zstd.get_frame_parameters(data, format=self._zstd_format).dict_id
        with self.get_decompressor(dict_id) as decompressor:
            return decompressor.decompress(data)


class ThreadLocalZstdManager(BaseZstdManager):
    """
    Thread-safe manager using thread-local storage for compressor/decompressor instances.

    Uses thread-local storage to ensure each thread gets its own compressor/
    decompressor instances, avoiding thread-safety issues while maintaining
    shared dictionary storage for efficiency.

    Usage:
        manager = ThreadLocalZstdManager()
        manager.register_dictionary(dict_bytes, domains=["users", "posts"])
        manager.set_default_dictionary(default_dict_bytes)

        compressed_data = manager.compress(data, domain)
        decompressed_data = manager.decompress(compressed_data)
    """

    def __init__(
        self,
        compression_level: int = 9,
        zstd_format: int = zstd.FORMAT_ZSTD1_MAGICLESS,
    ) -> None:
        super().__init__(compression_level, zstd_format)
        self._local = threading.local()

    @contextmanager
    def get_compressor(
        self, dict_id: int | None
    ) -> Generator[zstd.ZstdCompressor, None, None]:
        """Get or create thread-local compressor for a dictionary."""
        if not hasattr(self._local, "cache"):
            self._local.cache = ZstdThreadCache()

        if not (compressor := self._local.cache.get_compressor(dict_id)):
            dict_data = self._dictionaries.get_dictionary(dict_id) if dict_id else None
            compressor = zstd.ZstdCompressor(
                dict_data=dict_data,
                compression_params=self._compression_params,
            )
            self._local.cache.set_compressor(dict_id, compressor)

        yield compressor

    @contextmanager
    def get_decompressor(
        self, dict_id: int | None
    ) -> Generator[zstd.ZstdDecompressor, None, None]:
        """Get or create thread-local decompressor for a dictionary."""
        if not hasattr(self._local, "cache"):
            self._local.cache = ZstdThreadCache()

        if not (decompressor := self._local.cache.get_decompressor(dict_id)):
            dict_data = self._dictionaries.get_dictionary(dict_id) if dict_id else None
            decompressor = zstd.ZstdDecompressor(
                dict_data=dict_data,
                format=self._zstd_format,
            )
            self._local.cache.set_decompressor(dict_id, decompressor)

        yield decompressor


class PooledZstdManager(BaseZstdManager):
    """
    Pool-based manager for ZstdCompressor/ZstdDecompressor instances.

    Maintains pools of compressor/decompressor instances for each dictionary,
    using a context manager API for explicit resource control.

    Usage:
        manager = PooledZstdManager()
        manager.register_dictionary(dict_bytes, domains=["users"])

        # Context manager API for explicit control
        with manager.get_compressor(domain) as compressor:
            compressed = compressor.compress(data)

        # Or use convenience methods
        compressed = manager.compress(data, domain)
        decompressed = manager.decompress(compressed_data)
    """

    def __init__(
        self,
        compression_level: int = 9,
        zstd_format: int = zstd.FORMAT_ZSTD1_MAGICLESS,
    ) -> None:
        super().__init__(compression_level, zstd_format)
        self._compressor_pools: dict[int, Deque[zstd.ZstdCompressor]] = {}
        self._decompressor_pools: dict[int, Deque[zstd.ZstdDecompressor]] = {}
        self._create_pools_for_dict(0)

    def _create_pools_for_dict(self, dict_id: int) -> None:
        if dict_id not in self._compressor_pools:
            self._compressor_pools[dict_id] = deque()
        if dict_id not in self._decompressor_pools:
            self._decompressor_pools[dict_id] = deque()

    def register_dictionary(
        self,
        dictionary: bytes | Path,
        domains: list[str],
    ) -> int:
        """Register a dictionary for specific domains."""
        dict_id = super().register_dictionary(dictionary, domains)
        self._create_pools_for_dict(dict_id)
        return dict_id

    def set_default_dictionary(self, dictionary: bytes | Path) -> int:
        """Set the default dictionary for keys without a domain."""
        dict_id = super().set_default_dictionary(dictionary)
        self._create_pools_for_dict(dict_id)
        return dict_id

    @contextmanager
    def get_compressor(
        self, dict_id: int | None
    ) -> Generator[zstd.ZstdCompressor, None, None]:
        """Get a compressor from the pool (context manager)."""
        pool = self._compressor_pools.get(dict_id or 0)
        if pool is None:
            raise ValueError(f"Unknown dictionary id: {dict_id}")

        try:
            compressor = pool.popleft()
        except IndexError:
            dict_data = self._dictionaries.get_dictionary(dict_id) if dict_id else None
            compressor = zstd.ZstdCompressor(
                dict_data=dict_data,
                compression_params=self._compression_params,
            )

        try:
            yield compressor
        finally:
            pool.append(compressor)

    @contextmanager
    def get_decompressor(
        self, dict_id: int | None
    ) -> Generator[zstd.ZstdDecompressor, None, None]:
        """Get a decompressor from the pool (context manager)."""
        pool = self._decompressor_pools.get(dict_id or 0)
        if pool is None:
            raise ValueError(f"Unknown dictionary id: {dict_id}")

        try:
            decompressor = pool.popleft()
        except IndexError:
            dict_data = self._dictionaries.get_dictionary(dict_id) if dict_id else None
            decompressor = zstd.ZstdDecompressor(
                dict_data=dict_data,
                format=self._zstd_format,
            )
        try:
            yield decompressor
        finally:
            pool.append(decompressor)
