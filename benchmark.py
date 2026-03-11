import gc
import threading
import time
from collections.abc import Callable
from typing import Any

import click

from meta_memcache.cache_client import CacheClient
from meta_memcache.compression.zstd_manager import PooledZstdManager
from meta_memcache.configuration import (
    ServerAddress,
    build_server_pool,
    connection_pool_factory_builder,
    default_key_encoder,
)
from meta_memcache.connection.providers import (
    HashRingConnectionPoolProvider,
    NonConsistentHashPoolProvider,
)
from meta_memcache.executors.default import DefaultExecutor
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.routers.default import DefaultRouter
from meta_memcache.serializer import ZstdSerializer

NUM_KEYS = 200
# 10 out of 200 keys (5%) are large
LARGE_KEY_INDICES = frozenset(range(0, NUM_KEYS, NUM_KEYS // 10))
SMALL_VALUE_MIN = 80
SMALL_VALUE_MAX = 250
LARGE_VALUE_SIZE = 100_000


def _make_value(key_index: int) -> bytes:
    """Generate a deterministic value for a key index."""
    if key_index in LARGE_KEY_INDICES:
        # Large value: 100KB
        chunk = f"large-val-{key_index:04d}-".encode()
        return (chunk * (LARGE_VALUE_SIZE // len(chunk) + 1))[:LARGE_VALUE_SIZE]
    else:
        # Small value: 80-250 bytes, deterministic size based on key index
        size = SMALL_VALUE_MIN + (key_index * 7) % (
            SMALL_VALUE_MAX - SMALL_VALUE_MIN + 1
        )
        chunk = f"val-{key_index:04d}-".encode()
        return (chunk * (size // len(chunk) + 1))[:size]


class Benchmark:
    __slots__ = (
        "server",
        "consistent_sharding",
        "concurrency",
        "runs",
        "ops_per_run",
        "client",
        "with_gc",
    )

    def __init__(
        self,
        server: str,
        consistent_sharding: bool,
        concurrency: int,
        runs: int,
        ops_per_run: int,
        with_gc: bool,
    ) -> None:
        self.server = server
        self.consistent_sharding = consistent_sharding
        self.concurrency = concurrency
        self.runs = runs
        self.ops_per_run = ops_per_run
        self.with_gc = with_gc
        self.client = self._build_client()

    def _build_client(self) -> CacheApi:
        try:
            host, port_str = self.server.split(":")
            port = int(port_str)
        except ValueError:
            raise ValueError("Server must be in the format <IP>:<PORT>")

        connection_pool_factory_fn = connection_pool_factory_builder(
            initial_pool_size=self.concurrency, max_pool_size=self.concurrency * 5
        )

        server_pool = build_server_pool(
            servers=[ServerAddress(host, port)],
            connection_pool_factory_fn=connection_pool_factory_fn,
        )

        if self.consistent_sharding:
            pool_provider = HashRingConnectionPoolProvider(server_pool=server_pool)
        else:
            pool_provider = NonConsistentHashPoolProvider(server_pool=server_pool)

        executor = DefaultExecutor(
            serializer=ZstdSerializer(
                zstd_manager=PooledZstdManager(),
                compression_threshold=1_000_000_000,  # effectively disable compression
            ),
            key_encoder_fn=default_key_encoder,
            raise_on_server_error=True,
        )
        router = DefaultRouter(
            pool_provider=pool_provider,
            executor=executor,
        )
        return CacheClient(router=router)

    def populate(self) -> None:
        """Pre-populate keys: 80% of the key space with deterministic values."""
        populated = int(NUM_KEYS * 0.8)
        print(
            f"Populating {populated}/{NUM_KEYS} keys "
            f"(95% small {SMALL_VALUE_MIN}-{SMALL_VALUE_MAX}B, "
            f"5% large {LARGE_VALUE_SIZE // 1000}KB)..."
        )
        for i in range(populated):
            self.client.set(f"key{i}", _make_value(i), ttl=300)
        print(f"Done. {populated} keys set.")

    def _run_phase(
        self, name: str, fn: Callable[[int], Any], ops_per_run: int | None = None
    ) -> None:
        """Run a benchmark phase with multiple threads and report results."""
        ops_per_run = ops_per_run or self.ops_per_run
        total = self.runs * ops_per_run * self.concurrency
        print(f"\n--- {name} ---")

        def worker() -> None:
            count = 0
            for _ in range(self.runs):
                start_time = time.perf_counter()
                for _ in range(ops_per_run):
                    fn(count)
                    count += 1
                elapsed_time = time.perf_counter() - start_time
                rps = ops_per_run / elapsed_time
                us = elapsed_time / ops_per_run * 1_000_000
                print(f"  {name}: {rps:,.0f} RPS / {us:.2f} us/req")

        tasks = [threading.Thread(target=worker) for _ in range(self.concurrency)]
        start = time.perf_counter()
        for t in tasks:
            t.start()
        for t in tasks:
            t.join()
        elapsed = time.perf_counter() - start

        overall_rps = total / elapsed
        overall_us = elapsed / total * 1_000_000
        print(
            f"  {name} total: {total / 1_000_000:.2f}M in {elapsed:.2f}s "
            f"=> {overall_rps:,.0f} RPS / {overall_us:.2f} us/req"
        )

    def run(self) -> None:
        total_per_phase = self.runs * self.ops_per_run * self.concurrency
        print("=== Starting benchmark ===")
        print(f" - server: {self.server}")
        print(f" - consistent_sharding: {'ON' if self.consistent_sharding else 'OFF'}")
        print(f" - concurrency: {self.concurrency} threads")
        print(
            f" - per phase: {total_per_phase / 1_000_000:.2f}M requests "
            f"({self.runs} runs x {self.ops_per_run:,} ops x {self.concurrency} threads)"
        )
        print(f" - key space: {NUM_KEYS} keys")
        print(f" - GC: {'enabled' if self.with_gc else 'disabled'}")

        gc.collect()
        if not self.with_gc:
            gc.disable()

        # Phase 1: SET (also populates data for the GET phase)
        self.populate()
        gc_before = gc.get_count()
        self._run_phase(
            "SET",
            lambda count: self.client.set(
                f"key{count % NUM_KEYS}",
                _make_value(count % NUM_KEYS),
                ttl=300,
            ),
        )
        gc_after_set = gc.get_count()

        # Phase 2: GET (80% hits, 20% misses since we populated 80%)
        self._run_phase(
            "GET",
            lambda count: self.client.get(
                f"key{count % NUM_KEYS}",
            ),
        )
        gc_after_get = gc.get_count()

        # Phase 3: MULTI_GET (5 keys per call)
        keys_per_multiget = 5

        def multi_get_fn(count: int) -> None:
            base = count % NUM_KEYS
            keys = [f"key{(base + i) % NUM_KEYS}" for i in range(keys_per_multiget)]
            self.client.multi_get(keys=keys)

        self._run_phase(
            "MULTI_GET(5)",
            multi_get_fn,
            ops_per_run=self.ops_per_run // keys_per_multiget,
        )
        gc_after_mget = gc.get_count()

        # Phase 4: DELETE
        self._run_phase(
            "DELETE",
            lambda count: self.client.delete(
                f"key{count % NUM_KEYS}",
            ),
        )
        gc_after_del = gc.get_count()

        # Collect before re-enabling GC to see what accumulated during the benchmark
        collected = gc.collect()
        if not self.with_gc:
            gc.enable()

        print("\n--- GC tracked objects (gen0, gen1, gen2) ---")
        print(f"  before SET:      {gc_before}")
        print(f"  after SET:       {gc_after_set}")
        print(f"  after GET:       {gc_after_get}")
        print(f"  after MULTI_GET: {gc_after_mget}")
        print(f"  after DELETE:    {gc_after_del}")
        print(f"  gc.collect() found {collected} unreachable (cyclic garbage)")
        print("\n=== Benchmark finished ===")


@click.command()
@click.option("--server", required=True, help="Server address as <IP>:<PORT>.")
@click.option("--consistent-sharding/--no-consistent-sharding", default=True)
@click.option("--concurrency", default=1, help="Number of threads [1 by default].")
@click.option("--runs", default=10, help="Number of runs [10 by default].")
@click.option(
    "--ops-per-run",
    default=100_000,
    help="Number of operations per run [100K by default].",
)
@click.option(
    "--gc/--no-gc",
    is_flag=True,
    default=False,
    help="Enable/disable GC (disable by default).",
)
def cli(
    server: str,
    consistent_sharding: bool = True,
    concurrency: int = 1,
    runs: int = 10,
    ops_per_run: int = 100_000,
    gc: bool = False,
) -> None:
    Benchmark(
        server=server,
        consistent_sharding=consistent_sharding,
        concurrency=concurrency,
        runs=runs,
        ops_per_run=ops_per_run,
        with_gc=gc,
    ).run()


if __name__ == "__main__":
    cli()
