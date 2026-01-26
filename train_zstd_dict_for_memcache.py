#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "click",
#   "zstandard",
#   "meta-memcache",
#   "tabulate",
# ]
# ///
"""Train Zstd dictionary from memcache data.

Example Usages:

* Dump data from memcache servers into a tar file:
    ./train_zstd_dict_for_memcache.py dump \
        --min-storage-pct 1 \
        --sample-rate .5 \
        --threads 64 \
        --min-value-size 64 \
        --output ./dump.tar \
        <servers>

# Note: Replace <servers> with your memcache servers, e.g.
$(seq -f "memcache-server-%02g.local:112211" 5)

* Train a dictionary from the tar file:
    ./train_zstd_dict_for_memcache.py train \
        --dict-size 102400 \
        --input ./dump.tar \
        --output ./custom_dict.zstd \
        --control-pct 10
"""

import random
import socket
import time
import tarfile
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from threading import Thread

import zlib
import click
from tabulate import tabulate
import hashlib
import zstandard as zstd
from meta_memcache import CacheClient
from meta_memcache.configuration import ServerAddress, connection_pool_factory_builder
from meta_memcache.serializer import ZstdSerializer
from meta_memcache.compression.zstd_manager import ThreadLocalZstdManager


class ZstdNotUnpickleSerializer(ZstdSerializer):
    """Decompress but don't unpickle."""

    def unserialize(self, data, encoding_id):
        if encoding_id & self.ZLIB_COMPRESSED:
            data = zlib.decompress(data)
        elif encoding_id & self.ZSTD_COMPRESSED:
            data = self._zstd_manager.decompress(data)
        return bytes(data)


@dataclass
class SlabInfo:
    slab_id: int
    chunk_size: int
    used_chunks: int
    total_chunks: int


def get_slab_stats(host: str, port: int) -> dict[int, SlabInfo]:
    """Get slab stats via text protocol, return dict of SlabInfo."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(b"stats slabs\r\nquit\r\n")

    response = b""
    while True:
        data = sock.recv(4096)
        if not data:
            break
        response += data

    slabs = {}
    for line in response.decode("utf-8").split("\r\n"):
        if line.startswith("STAT ") and ":" in line:
            parts = line.split()
            slab_part = parts[1].split(":")
            slab_id = int(slab_part[0])
            field = slab_part[1]
            value = int(parts[2])

            if slab_id not in slabs:
                slabs[slab_id] = {"slab_id": slab_id}
            slabs[slab_id][field] = value

    result = {}
    for sid, data in slabs.items():
        if "chunk_size" in data and "used_chunks" in data:
            result[sid] = SlabInfo(
                slab_id=sid,
                chunk_size=data["chunk_size"],
                used_chunks=data["used_chunks"],
                total_chunks=data.get("total_chunks", 0),
            )
    return result


def get_keys_from_slab(
    host: str, port: int, slab_id: int, limit: int = 250000
) -> tuple[list[str], int]:
    """Get keys from slab via cachedump."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall(f"stats cachedump {slab_id} {limit}\r\nquit\r\n".encode())

    response = b""
    while True:
        data = sock.recv(4096)
        if not data:
            break
        response += data

    keys = []
    size = 0
    for line in response.decode("utf-8").split("\r\n"):
        # Format:
        # ITEM <key> [<size> b; <expiry> s]
        if line.startswith("ITEM "):
            parts = line.split()
            value_size = int(parts[2][1:])
            if value_size >= 32:
                # Skip tiny values, unlikely to decompress to
                # larger than 64 bytes minimum viable compression
                # size for a dictionary.
                key = parts[1]
                size += value_size
                keys.append(key)
    return keys, size


blacklist = {"\\", "/", ":", "*", "?", '"', "<", ">", "|", "\0"}


def sanitize_key(key: str) -> str:
    """Sanitize key for use as filename."""
    return "".join(c for c in key if c not in blacklist)

    return hashlib.md5(key.encode()).hexdigest()


@click.group()
def cli():
    """Train Zstd dictionary from memcache data."""
    pass


def tar_writer_thread(
    tar_path: Path, write_queue: deque[tuple[str, bytes] | None]
) -> None:
    """Writer thread that reads from queue and writes to tar file."""
    with tarfile.open(tar_path, "w") as tar:
        while True:
            # Try to get an item from the queue
            try:
                item = write_queue.popleft()
            except IndexError:
                # Queue is empty, sleep a bit and retry
                time.sleep(0.001)
                continue

            # Check for sentinel value
            if item is None:
                break

            arcname, data = item

            # Create tarinfo and write to tar
            tarinfo = tarfile.TarInfo(name=arcname)
            tarinfo.size = len(data)
            tarinfo.mtime = time.time()
            tar.addfile(tarinfo, io.BytesIO(data))


def fetch_and_write(
    keys_by_server: dict[tuple[str, int], list[str]],
    min_value_size: int,
    tar_path: Path,
    threads: int,
    chunk_size: int = 50,
) -> None:
    """Fetch values and write to tar file from threads."""

    # Create shared deque for passing data to writer thread
    write_queue = deque()

    # Start tar writer thread
    writer = Thread(target=tar_writer_thread, args=(tar_path, write_queue))
    writer.start()

    def fetch_and_write_chunk(host, port, cache_client, chunk):
        """Fetch a chunk of keys and add to write queue."""
        errors = 0
        misses = 0
        successes = 0
        filtered = 0
        size = 0
        for key in chunk:
            try:
                result = cache_client.get(key)
                if not result:
                    misses += 1
                elif len(result) >= min_value_size:
                    successes += 1
                    # Add to write queue instead of writing to disk
                    arcname = f"{host}_{port}/{sanitize_key(key)}.bin"
                    write_queue.append((arcname, result))
                    size += len(result)
                else:
                    filtered += 1
            except Exception as e:
                click.echo(f"Error fetching {key}: {e}", err=True)
                errors += 1

        return errors, misses, successes, filtered, size

    jobs: list[tuple[CacheClient, list[str]]] = []
    zstd_manager = ThreadLocalZstdManager()
    for (host, port), keys in keys_by_server.items():
        cache_client = CacheClient.cache_client_from_servers(
            servers=[ServerAddress(host=host, port=port)],
            connection_pool_factory_fn=connection_pool_factory_builder(),
            serializer=ZstdNotUnpickleSerializer(zstd_manager),
        )

        for i in range(0, len(keys), chunk_size):
            jobs.append((host, port, cache_client, keys[i : i + chunk_size]))

    total_errors = 0
    total_misses = 0
    total_successes = 0
    total_filtered = 0
    total_size = 0
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(fetch_and_write_chunk, *job) for job in jobs]
        with click.progressbar(length=len(jobs), label="Fetching:") as bar:
            for future in as_completed(futures):
                job_errors, job_misses, job_successes, job_filtered, job_size = (
                    future.result()
                )
                bar.update(1)
                total_errors += job_errors
                total_misses += job_misses
                total_successes += job_successes
                total_filtered += job_filtered
                total_size += job_size

    # Signal writer thread to stop and wait for it to finish
    write_queue.append(None)
    writer.join()

    click.echo(f"\nTotal errors: {total_errors}")
    click.echo(f"Total misses: {total_misses}")
    click.echo(f"Total successes: {total_successes}")
    click.echo(f"Total filtered: {total_filtered}")
    click.echo(f"Total downloaded size: {total_size / 1024 / 1024:.2f} MB")


@cli.command()
@click.option(
    "--min-storage-pct",
    type=float,
    default=5.0,
    help="Min %% storage for slab selection",
)
@click.option(
    "--sample-rate", type=float, default=0.1, help="Fraction of keys to sample"
)
@click.option("--threads", type=int, default=16, help="Worker threads")
@click.option(
    "--min-value-size", type=int, default=64, help="Min value size in bytes to include"
)
@click.option("--output", type=click.Path(), required=True, help="Output tar file path")
@click.argument("servers", nargs=-1)
def dump(
    min_storage_pct: float,
    sample_rate: float,
    threads: int,
    min_value_size: int,
    output: Path,
    servers: list[str],
) -> None:
    """Dump cache values to tar file with structure: <host>_<port>/<key>.bin"""

    servers = [(s.split(":")[0], int(s.split(":")[1])) for s in servers]
    threads,
    min_value_size,
    output,
    servers,
):
    """Dump cache values to tar file with structure: <host>_<port>/<key>.bin"""

    servers = [(s.split(":")[0], int(s.split(":")[1])) for s in servers]
    tar_path = Path(output)

    # Get slab stats for first server
    host, port = servers[0]
    click.echo(f"Fetching slab stats from {host}:{port}...")
    slabs = get_slab_stats(host, port)

    # Calculate storage and select slabs
    total_storage = sum(s.used_chunks * s.chunk_size for s in slabs.values())
    selected = []

    click.echo(f"\nTotal storage: {total_storage / 1024 / 1024:.2f} MB\n")
    click.echo(
        f"{'Slab':<8} {'Chunk Size':<12} {'Used':<10} {'Storage MB':<12} {'Storage %':<10}"
    )
    click.echo("-" * 60)

    for slab in sorted(
        slabs.values(), key=lambda s: s.used_chunks * s.chunk_size, reverse=True
    ):
        storage = slab.used_chunks * slab.chunk_size
        pct = (storage / total_storage) * 100
        click.echo(
            f"{slab.slab_id:<8} {slab.chunk_size:<12} {slab.used_chunks:<10} {storage / 1024 / 1024:<12.2f} {pct:<10.2f}"
        )
        if pct >= min_storage_pct:
            selected.append(slab)

    click.echo(
        f"\nSelected {len(selected)} slabs (>= {min_storage_pct}% storage): {', '.join(f'{slab.slab_id}' for slab in selected)}\n"
    )

    if not click.confirm(
        "Continue with these slabs? (Use --min-storage-pct to adjust selection)",
        default=True,
    ):
        click.echo("Aborted.")
        return

    # Collect all keys by server
    keys_by_server: dict[tuple[str, int], list[str]] = {}
    total_size = 0
    for host, port in servers:
        for slab in selected:
            keys, size = get_keys_from_slab(host, port, slab.slab_id)
            sampled = (
                random.sample(keys, int(len(keys) * sample_rate))
                if sample_rate < 1.0
                else keys
            )
            click.echo(
                f"{host}:{port} Slab {slab.slab_id}: {len(keys)} keys, sampled {len(sampled)}, size {size * sample_rate / 1024 / 1024:.2f} MB"
            )

            keys_by_server.setdefault((host, port), []).extend(sampled)
            total_size += size

    click.echo(f"\nTotal keys to fetch: {sum(len(x) for x in keys_by_server.values())}")
    click.echo(f"Total size to fetch: {total_size * sample_rate / 1024 / 1024:.2f} MB")

    click.echo("\nFetching and writing to tar...")
    fetch_and_write(keys_by_server, min_value_size, tar_path, threads)

    click.echo(f"\nDump complete. Data saved to {tar_path}")


def benchmark_compression(
    manager: ThreadLocalZstdManager, data: list[bytes], domain: str | None, times=10
) -> tuple[float, float, float]:
    """Benchmark compression speed and ratio.
    Returns compression speed in MB/s, decompression speed in MB/s, and compression ratio."""
    data_size = sum(len(d) for d in data)
    comp_data = [manager.compress(d, domain=domain) for d in data]
    comp_size = sum(len(v) for v in comp_data)

    start = time.perf_counter()
    for _ in range(times):
        for d in data:
            manager.compress(d, domain=domain)
    comp_time = time.perf_counter() - start

    start = time.perf_counter()
    for _ in range(times):
        for d in comp_data:
            manager.decompress(d)
    decomp_time = time.perf_counter() - start

    # Compression speed in MB/s
    comp_speed = data_size * times / 1024 / 1024 / comp_time
    # Decompression speed in MB/s
    decomp_speed = data_size * times / 1024 / 1024 / decomp_time
    # Compression ratio
    comp_ratio = (1 - comp_size / data_size) * 100
    return comp_speed, decomp_speed, comp_ratio


@cli.command()
@click.option("--dict-size", type=int, default=102400, help="Dictionary size in bytes")
@click.option(
    "--input",
    type=click.Path(exists=True),
    required=True,
    help="Input tar file with dumped data",
)
@click.option(
    "--output", type=click.Path(), required=True, help="Output dictionary path"
)
@click.option(
    "--control-pct",
    type=float,
    default=0.0,
    help="Percentage of data to reserve for benchmarking (0-100)",
)
@click.option(
    "--min-level", type=int, default=1, help="Minimum compression level for benchmark"
)
@click.option(
    "--max-level", type=int, default=11, help="Maximum compression level for benchmark"
)
@click.option(
    "--times",
    type=int,
    default=10,
    help="Number of times to benchmark each compression level",
)
def train(dict_size: int, input: Path, output: Path, control_pct: float, min_level: int, max_level: int, times: int) -> None:
    """Train Zstd dictionary from dumped tar file, optionally benchmark on control set."""

    tar_path = Path(input)

    if not tar_path.exists():
        click.echo(f"Error: Tar file not found: {tar_path}")
        return

    # Load all data from tar file
    click.echo(f"Loading data from {tar_path}...")
    all_data = []

    with tarfile.open(tar_path, "r") as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith(".bin"):
                f = tar.extractfile(member)
                if f:
                    all_data.append(f.read())

    click.echo(
        f"Loaded {len(all_data)} values, {sum(len(d) for d in all_data) / 1024 / 1024:.2f} MB\n"
    )

    if not all_data:
        click.echo("Error: No data found")
        return

    # Split into train/control if needed
    training_data = all_data
    total_original = sum(len(d) for d in all_data)
    control_data = []

    if control_pct > 0:
        random.shuffle(all_data)
        split_idx = int(len(all_data) * (1 - control_pct / 100))
        training_data = all_data[:split_idx]
        control_data = all_data[split_idx:]

        click.echo(
            f"Split: {len(training_data)} training, {len(control_data)} control\n"
        )

    # Train dictionary
    click.echo(f"{'=' * 80}")
    click.echo("TRAINING DICTIONARY")
    click.echo(f"{'=' * 80}")
    click.echo(
        f"Training dictionary (size={dict_size} bytes) with {len(training_data)} samples, {total_original / 1024 / 1024:.2f} MB..."
    )
    dict_data = zstd.train_dictionary(dict_size, training_data)
    Path(output).write_bytes(dict_data.as_bytes())
    click.echo(f"DONE! Dictionary saved to {output}\n")

    # Benchmark if control data exists
    if control_data:
        click.echo("Benchmarking on control set...\n")

        total_original = sum(len(d) for d in control_data)

        click.echo(f"{'=' * 80}")
        click.echo("COMPRESSION BENCHMARK")
        click.echo(f"{'=' * 80}")
        click.echo(
            f"Control set: {len(control_data)} samples, {total_original / 1024 / 1024:.2f} MB, {times} times "
        )

        results = []
        with click.progressbar(
            range(min_level, max_level + 1), label="Benchmarking:"
        ) as levels:
            for level in levels:
                manager = ThreadLocalZstdManager(
                    compression_level=level, zstd_format=zstd.FORMAT_ZSTD1_MAGICLESS
                )
                manager.register_dictionary(dict_data.as_bytes(), domains=["dict"])

                # Without dictionary
                (comp_speed, decomp_speed, ratio) = benchmark_compression(
                    manager, control_data, domain=None, times=times
                )

                # With dictionary
                (
                    comp_speed_dict,
                    decomp_speed_dict,
                    ratio_dict,
                ) = benchmark_compression(
                    manager, control_data, domain="dict", times=times
                )

                results.append(
                    [
                        level,
                        f"{ratio:.2f}% vs {ratio_dict:.2f}%",
                        f"{ratio_dict - ratio:+0.2f}%",
                        f"{comp_speed:.2f}MB/s vs {comp_speed_dict:.2f}MB/s",
                        f"{comp_speed_dict / comp_speed:.2f}x",
                        f"{decomp_speed:.2f}MB/s vs {decomp_speed_dict:.2f}MB/s",
                        f"{decomp_speed_dict / decomp_speed:.2f}x",
                    ]
                )

        click.echo(
            tabulate(
                results,
                headers=[
                    "Level",
                    "Compression ratio",
                    "Improvement",
                    "Compression Speed",
                    "Improvement",
                    "Decompression Speed",
                    "Improvement",
                ],
            )
        )


if __name__ == "__main__":
    cli()
