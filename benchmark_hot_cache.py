"""
Micro-benchmark of the sqlite hot cache get() path: hits, misses,
revalidations of a stale entry, and stores.

Run against the working tree:
    uv run python benchmark_hot_cache.py --sizes 100,1000,100000

Compare git revisions (each one runs in its own worktree and venv):
    uv run python benchmark_hot_cache.py --rev main --rev HEAD --rev WORKTREE

WORKTREE is the uncommitted working tree. Add --python 3.11 to pick the
interpreter (it also changes the bundled sqlite version).
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

REPO = Path(__file__).resolve().parent
WORKTREE = "WORKTREE"
HOT_TTL = 1 << 30
REVALIDATE_TTL = 1


class FakeClient:
    """Serves `hot*` keys as hot, so the first get() promotes them."""

    on_write_failure = None

    def __init__(self, value: bytes) -> None:
        from meta_memcache.protocol import ResponseFlags, Value

        self._hot = Value(
            size=len(value),
            value=value,
            flags=ResponseFlags(fetched=True, last_access=1),
        )
        self._cold = Value(
            size=len(value),
            value=value,
            flags=ResponseFlags(fetched=True, last_access=9999),
        )
        self.calls = 0

    def meta_get(self, key: Any, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        return self._hot if key.key.startswith("hot") else self._cold

    def meta_multiget(self, keys: List[Any], *args: Any, **kwargs: Any) -> Any:
        return {key: self.meta_get(key) for key in keys}

    # Older revisions read through _get()/_multi_get()
    _get = meta_get

    def _multi_get(self, keys: List[Any], *args: Any, **kwargs: Any) -> Any:
        return self.meta_multiget(keys)


def build_cache(
    client: FakeClient, db_path: str, cache_ttl: int = HOT_TTL, recreate: bool = True
) -> Any:
    from meta_memcache.extras.probabilistic_hot_cache_sqlite import (
        HotCacheDBConfig,
        SqliteProbabilisticHotCache,
    )

    db = HotCacheDBConfig.initialize(db_path, max_size_bytes=1 << 31, recreate=recreate)
    return SqliteProbabilisticHotCache(
        client=client,
        db=db,
        cache_ttl=cache_ttl,
        max_last_access_age_seconds=10,
        probability_factor=1,
        # The clock below jumps a ttl per call: keep the purge out of the loop.
        purge_interval_seconds=1 << 40,
    )


class Clock:
    """Stands in for time.time(), so revalidations don't wait on a real ttl."""

    def __init__(self) -> None:
        self.now = time.time()

    def __call__(self) -> float:
        return self.now


def time_revalidation(value: bytes, db_path: str, args: argparse.Namespace) -> float:
    from meta_memcache import Key

    client = FakeClient(value)
    # Same db as the hits, so the table holds the same rows
    cache = build_cache(client, db_path, cache_ttl=REVALIDATE_TTL, recreate=False)
    key = "hot_revalidated"
    clock = Clock()
    real_time = time.time
    time.time = clock  # type: ignore[assignment]
    try:
        cache._store_entry(Key(key), value)

        def revalidate() -> None:
            # Lands right as the entry goes stale: every call wins the
            # election, refetches and stores the value again.
            clock.now += REVALIDATE_TTL
            cache.get(key)

        calls = client.calls
        revalidate()
        assert client.calls == calls + 1, "the lookup didn't revalidate"
        return time_per_call(revalidate, args.min_seconds, args.repeats)
    finally:
        time.time = real_time  # type: ignore[assignment]


def time_per_call(fn: Callable[[], Any], min_seconds: float, repeats: int) -> float:
    fn()
    n = 1
    while True:
        start = time.perf_counter()
        for _ in range(n):
            fn()
        if time.perf_counter() - start >= min_seconds / repeats:
            break
        n *= 2
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        for _ in range(n):
            fn()
        samples.append((time.perf_counter() - start) / n)
    return statistics.median(samples)


def run_local(args: argparse.Namespace) -> List[Dict[str, Any]]:
    results = []
    with tempfile.TemporaryDirectory(dir=args.db_dir) as tmp:
        for size in args.sizes:
            value = os.urandom(size)
            cache = build_cache(FakeClient(value), f"{tmp}/hot.db")
            for i in range(args.rows):
                cache.get(f"hot{i}")
            hit_key = f"hot{args.rows // 2}"
            assert cache.get(hit_key) == value
            from meta_memcache import Key

            store_key = Key(hit_key)
            results.append(
                {
                    "size": size,
                    "rows": args.rows,
                    "hit_us": 1e6
                    * time_per_call(
                        lambda: cache.get(hit_key), args.min_seconds, args.repeats
                    ),
                    "miss_us": 1e6
                    * time_per_call(
                        lambda: cache.get("cold"),
                        args.min_seconds,
                        args.repeats,
                    ),
                    "revalidate_us": 1e6
                    * time_revalidation(value, f"{tmp}/hot.db", args),
                    "store_us": 1e6
                    * time_per_call(
                        lambda: cache._store_entry(store_key, value),
                        args.min_seconds,
                        args.repeats,
                    ),
                }
            )
    return results


def run_rev(rev: str, args: argparse.Namespace, tmp: str) -> List[Dict[str, Any]]:
    if rev == WORKTREE:
        tree = REPO
    else:
        tree = Path(tmp) / rev.replace("/", "_")
        subprocess.run(
            [
                "git",
                "-C",
                str(REPO),
                "worktree",
                "add",
                "-q",
                "--detach",
                str(tree),
                rev,
            ],
            check=True,
        )
    cmd = ["uv", "run", "-q", "--project", str(tree)]
    if args.python:
        cmd += ["--python", args.python]
    cmd += [
        "python",
        str(Path(__file__).resolve()),
        "--json",
        "--sizes",
        ",".join(map(str, args.sizes)),
        "--rows",
        str(args.rows),
        "--min-seconds",
        str(args.min_seconds),
        "--repeats",
        str(args.repeats),
    ]
    if args.db_dir:
        cmd += ["--db-dir", args.db_dir]
    # Older revisions don't install the project, so import it from source
    env = {**os.environ, "PYTHONPATH": str(tree / "src")}
    env.pop("VIRTUAL_ENV", None)
    out = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, text=True)
    if out.returncode:
        sys.exit(f"Benchmark failed for {rev}, see the error above")
    return [{"rev": rev, **row} for row in json.loads(out.stdout)]


def print_table(rows: List[Dict[str, Any]]) -> None:
    cols = [
        c
        for c in (
            "rev",
            "size",
            "rows",
            "hit_us",
            "miss_us",
            "revalidate_us",
            "store_us",
        )
        if c in rows[0]
    ]
    cells = [
        [f"{row[c]:.1f}" if isinstance(row[c], float) else str(row[c]) for c in cols]
        for row in rows
    ]
    widths = [max(len(c), *(len(r[i]) for r in cells)) for i, c in enumerate(cols)]
    print("  ".join(c.rjust(w) for c, w in zip(cols, widths)))
    for r in cells:
        print("  ".join(v.rjust(w) for v, w in zip(r, widths)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument(
        "--rev",
        action="append",
        default=[],
        help=f"git revision to compare, or {WORKTREE}",
    )
    parser.add_argument(
        "--sizes",
        default="100,1000,16000,100000,1000000",
        type=lambda s: [int(x) for x in s.split(",")],
    )
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument(
        "--min-seconds", type=float, default=1.0, help="time budget per measurement"
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--db-dir", help="where to put the db, eg: /dev/shm")
    parser.add_argument("--python", help="interpreter for --rev runs")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if not args.rev:
        rows = run_local(args)
    else:
        rows = []
        try:
            with tempfile.TemporaryDirectory() as tmp:
                for rev in args.rev:
                    rows += run_rev(rev, args, tmp)
        finally:
            subprocess.run(["git", "-C", str(REPO), "worktree", "prune"])
        rows.sort(key=lambda r: (r["size"], args.rev.index(r["rev"])))
    if args.json:
        json.dump(rows, sys.stdout)
    else:
        print_table(rows)


if __name__ == "__main__":
    main()
