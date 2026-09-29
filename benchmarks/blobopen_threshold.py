"""
Hit latency per value size, reading inline vs with blobopen, to pick
_INLINE_MAX_BYTES. Runs against the checked out code:
    uv run python benchmarks/blobopen_threshold.py --db-dir /dev/shm
"""

import argparse
import os
import statistics
import sys
import tempfile
import threading
import time
from typing import Any, List

from hot_cache import Scenario, build_cache, fresh_cache, time_per_call


def hits_per_second(cache: Any, threads: int, seconds: float, repeats: int) -> float:
    ops = [0] * threads
    ready = threading.Barrier(threads + 1)
    stop = threading.Event()

    def worker(n: int) -> None:
        ready.wait()
        while not stop.is_set():
            cache.get("hot0")
            ops[n] += 1

    workers = [threading.Thread(target=worker, args=(n,)) for n in range(threads)]
    for worker_thread in workers:
        worker_thread.start()
    ready.wait()
    rates = []
    for _ in range(repeats):
        before, start = sum(ops), time.perf_counter()
        time.sleep(seconds / repeats)
        rates.append((sum(ops) - before) / (time.perf_counter() - start))
    stop.set()
    for worker_thread in workers:
        worker_thread.join()
    return statistics.median(rates)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--sizes",
        default="16000,32000,64000,100000,150000,200000,300000,500000,"
        "1000000,1500000,2000000",
        type=lambda s: [int(x) for x in s.split(",")],
    )
    parser.add_argument("--threads", type=int, default=4)
    # 10 rows of 2MB fit a small /dev/shm
    parser.add_argument("--rows", type=int, default=10)
    parser.add_argument("--min-seconds", type=float, default=1.0)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--db-dir", help="where to put the db, eg: /dev/shm")
    parser.add_argument("--cache-kb", type=int, help="PRAGMA cache_size, in KB")
    parser.add_argument(
        "--checkpoint", action="store_true", help="empty the WAL before measuring"
    )
    parser.add_argument(
        "--reader",
        action="store_true",
        help="measure on a connection that didn't write the rows",
    )
    args = parser.parse_args()

    import sqlite3

    from meta_memcache.extras import probabilistic_hot_cache_sqlite as module

    if not hasattr(module, "_INLINE_MAX_BYTES"):
        sys.exit("This checkout has no blobopen read path (_INLINE_MAX_BYTES)")
    if not hasattr(sqlite3.Connection, "blobopen"):
        sys.exit("blobopen needs python 3.11+")
    print(f"python {sys.version.split()[0]}, sqlite {sqlite3.sqlite_version}")
    if args.cache_kb:
        connect = module.HotCacheDBConfig.connect

        def connect_with_cache_size(self: Any) -> sqlite3.Connection:
            conn = connect(self)
            conn.execute(f"PRAGMA cache_size = -{args.cache_kb}")
            return conn

        module.HotCacheDBConfig.connect = connect_with_cache_size  # type: ignore
    t = args.threads
    print(
        f"{'size':>8}  {'inline_us':>9}  {'blobopen_us':>11}"
        f"  {f'inline_{t}t_us':>13}  {f'blobopen_{t}t_us':>15}"
    )
    with tempfile.TemporaryDirectory(dir=args.db_dir) as tmp:
        for size in args.sizes:
            scenario = Scenario(str(size), [os.urandom(size)])

            def measure(limit: int) -> List[float]:
                module._INLINE_MAX_BYTES = limit
                cache = fresh_cache(scenario, f"{tmp}/hot.db", args)
                if args.checkpoint:
                    cache._get_conn().execute("PRAGMA wal_checkpoint(TRUNCATE)")
                writer = cache  # Kept open: the last close would empty the WAL
                if args.reader:
                    cache = build_cache(cache.client, f"{tmp}/hot.db", recreate=False)
                assert cache.get("hot0") == scenario.values[0]
                us = 1e6 * time_per_call(
                    lambda: cache.get("hot0"), args.min_seconds, args.repeats
                )
                rate = hits_per_second(cache, t, args.min_seconds, args.repeats)
                # Wall time per get(), with the threads competing for it
                del writer
                return [us, 1e6 / rate]

            # Always inline, then always blobopen
            inline_us, inline_mt_us = measure(sys.maxsize)
            blob_us, blob_mt_us = measure(0)
            print(
                f"{size:>8}  {inline_us:>9.1f}  {blob_us:>11.1f}"
                f"  {inline_mt_us:>13.1f}  {blob_mt_us:>15.1f}",
                flush=True,
            )


if __name__ == "__main__":
    main()
