import gc
import threading
import time
from typing import Any, Callable, Optional

import click
from meta_memcache.cache_client import CacheClient
from meta_memcache.configuration import (
    ServerAddress,
    build_server_pool,
    connection_pool_factory_builder,
    default_key_encoder,
)
from meta_memcache.connection.pool import ConnectionPool
from meta_memcache.connection.providers import (
    HashRingConnectionPoolProvider,
    NonConsistentHashPoolProvider,
)
from meta_memcache.executors.default import DefaultExecutor
from meta_memcache.interfaces.cache_api import CacheApi
from meta_memcache.routers.default import DefaultRouter
from meta_memcache.serializer import MixedSerializer
from meta_memcache.settings import DEFAULT_MARK_DOWN_PERIOD_S


class FakeSocket:
    def __init__(self, response: bytes) -> None:
        self.response = response
        self.response_size = len(response)

    def recv_into(
        self,
        buff: Any,
        size: Optional[int] = None,
        flags: Optional[int] = None,
    ) -> int:
        if size is not None:
            raise NotImplementedError("size is not implemented")

        buff[0 : self.response_size] = self.response
        return self.response_size

    def sendall(self, buff: Any, flags: Any = None) -> None:
        return None

    def setsockopt(self, *args: Any, **kwargs: Any) -> None:
        return None


def fake_connection_pool_factory_builder(
    fake_socket: FakeSocket,
    initial_pool_size: int = 1,
    max_pool_size: int = 3,
    mark_down_period_s: float = DEFAULT_MARK_DOWN_PERIOD_S,
    read_buffer_size: int = 4096,
) -> Callable[[ServerAddress], ConnectionPool]:
    def connection_pool_builder(server_address: ServerAddress) -> ConnectionPool:
        return ConnectionPool(
            server=str(server_address),
            socket_factory_fn=lambda *args, **kwargs: fake_socket,  # type: ignore
            initial_pool_size=initial_pool_size,
            max_pool_size=max_pool_size,
            mark_down_period_s=mark_down_period_s,
            read_buffer_size=read_buffer_size,
            version=server_address.version,
        )

    return connection_pool_builder


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
        if self.server:
            try:
                host, port_str = self.server.split(":")
                port = int(port_str)
            except ValueError:
                raise ValueError("Server must be in the format <IP>:<PORT>")

            connection_pool_factory_fn = connection_pool_factory_builder(
                initial_pool_size=self.concurrency, max_pool_size=self.concurrency * 5
            )
        else:
            host, port = "localhost", 11211
            connection_pool_factory_fn = fake_connection_pool_factory_builder(
                fake_socket=FakeSocket(response=b"VA 5 h1 f0 l6 t-1\r\nvalue\r\n"),
                initial_pool_size=self.concurrency,
                max_pool_size=self.concurrency * 5,
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
            serializer=MixedSerializer(),
            key_encoder_fn=default_key_encoder,
            raise_on_server_error=True,
        )
        router = DefaultRouter(
            pool_provider=pool_provider,
            executor=executor,
        )
        return CacheClient(router=router)

    def getter(self) -> None:
        count = 0
        for _ in range(self.runs):
            start_time = time.perf_counter()
            while True:
                self.client.get(f"key{count%200}")
                count += 1
                if count % self.ops_per_run == 0:
                    elapsed_time = time.perf_counter() - start_time
                    ops_per_sec = self.ops_per_run / elapsed_time
                    us = elapsed_time / self.ops_per_run * 1_000_000
                    print(f"Gets: {ops_per_sec:.2f} RPS / {us:.2f} us/req")
                    break

    def run(self) -> None:
        total = self.runs * self.ops_per_run * self.concurrency
        print("=== Starting benchmark ===")
        print(f" - server: {self.server or '<mocked>'}")
        print(f" - consistent_sharding: {'ON' if self.consistent_sharding else 'OFF'}")
        print(f" - concurrency: {self.concurrency} threads")
        print(f" - Requests: {total/1_000_000:.2f}M")
        print(f"    ({self.runs} runs of {self.ops_per_run} reqs per thread)")
        print(f" - GC: {'enabled' if self.with_gc else 'disabled'}")
        print()
        tasks = [threading.Thread(target=self.getter) for _ in range(self.concurrency)]

        if not self.with_gc:
            gc.disable()
        s = time.time()
        for t in tasks:
            t.start()

        for t in tasks:
            t.join()
        elapsed = time.time() - s
        print("GC stats:")
        if not self.with_gc:
            gc.enable()
        print(gc.get_stats())
        print(gc.get_count())
        print("GC collect:", gc.collect())
        print()
        print("=== Benchmark finished ===")
        print(f"Total: {total / 1_000_000:.2f}M requests in {elapsed:.2f}s")
        print(
            f"Overall: {total / elapsed:.2f} RPS / "
            f"{elapsed / total * 1_000_000:.2f} us/req"
        )


@click.command()
@click.option("--server", default="", help="Server address as <IP>:<PORT>.")
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
