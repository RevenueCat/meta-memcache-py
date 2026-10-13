"""
Micro-benchmark of the sqlite hot cache get() path: hits, misses,
revalidations of a stale entry, and stores.

reval_* gets the same value back, reval_changed_* a changed one; *_wal_kb is
the WAL written per revalidation. The mix row averages over --mix: size or
lo-hi (log-uniform) buckets with weights, eg: 100:70,4000-1000000:30.
mt_us is wall time per get() and mt_errors the sqlite errors, with --threads
threads sharing one cache and a 1s ttl so revalidations happen.

Run against the working tree:
    uv run python benchmarks/hot_cache.py --sizes 100,1000,100000

Compare git revisions (each one runs in its own worktree and venv):
    uv run python benchmarks/hot_cache.py --rev main --rev HEAD --rev WORKTREE

WORKTREE is the uncommitted working tree. Add --python 3.11 to pick the
interpreter (it also changes the bundled sqlite version).
"""

import argparse
import inspect
import json
import math
import os
import random
import re
import statistics
import subprocess
import sys
import sqlite3
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, NamedTuple

REPO = Path(__file__).resolve().parent.parent
WORKTREE = "WORKTREE"
HOT_TTL = 1 << 30
REVALIDATE_TTL = 1
WAL_CALLS = 20


class Scenario(NamedTuple):
    # key <name><i> holds values[i % len(values)]

    label: str
    values: List[bytes]

    def keys(self, name: str) -> List[str]:
        return [f"{name}{i}" for i in range(len(self.values))]


def key_index(key: str) -> int:
    digits = re.search(r"\d+$", key)
    return int(digits.group()) if digits else 0


class FakeClient:
    # hot* keys look hot, so the first get() promotes them

    on_write_failure = None

    def __init__(self, scenario: Scenario, changed: bool = False) -> None:
        from meta_memcache.protocol import ResponseFlags, Value

        def response(value: bytes, last_access: int) -> Any:
            return Value(
                size=len(value),
                value=value,
                flags=ResponseFlags(fetched=True, last_access=last_access),
            )

        variants = [
            [value, flip_last_byte(value)] if changed else [value]
            for value in scenario.values
        ]
        self._hot = [[response(v, 1) for v in vs] for vs in variants]
        self._cold = [response(vs[0], 9999) for vs in variants]
        self._reads: Dict[str, int] = {}
        self.calls = 0

    def meta_get(self, key: Any, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        i = key_index(key.key) % len(self._hot)
        if not key.key.startswith("hot"):
            return self._cold[i]
        # Per key, so keys revalidated in turn still alternate
        reads = self._reads.get(key.key, 0)
        self._reads[key.key] = reads + 1
        return self._hot[i][reads % len(self._hot[i])]

    def meta_multiget(self, keys: List[Any], *args: Any, **kwargs: Any) -> Any:
        return {key: self.meta_get(key) for key in keys}

    # Older revisions read through _get()/_multi_get()
    _get = meta_get

    def _multi_get(self, keys: List[Any], *args: Any, **kwargs: Any) -> Any:
        return self.meta_multiget(keys)


def build_cache(
    client: FakeClient,
    db_path: str,
    cache_ttl: int = HOT_TTL,
    recreate: bool = True,
    max_wal_bytes: int = 8 * 1024 * 1024,
    **cache_kwargs: Any,
) -> Any:
    from meta_memcache.extras.probabilistic_hot_cache_sqlite import (
        HotCacheDBConfig,
        SqliteProbabilisticHotCache,
    )

    db = HotCacheDBConfig.initialize(
        db_path, max_size_bytes=1 << 31, recreate=recreate, max_wal_bytes=max_wal_bytes
    )
    return SqliteProbabilisticHotCache(
        client=client,
        db=db,
        cache_ttl=cache_ttl,
        max_last_access_age_seconds=10,
        probability_factor=1,
        # The clock below jumps a ttl per call: keep the purge out of the loop.
        purge_interval_seconds=1 << 40,
        **cache_kwargs,
    )


def store_entry(cache: Any, key: Any, value: bytes) -> None:
    # 4.0 added the size argument
    if "size" in inspect.signature(cache._store_entry).parameters:
        cache._store_entry(key, value, len(value))
    else:
        cache._store_entry(key, value)


def fresh_cache(
    scenario: Scenario,
    db_path: str,
    args: argparse.Namespace,
    changed: bool = False,
    **kwargs: Any,
) -> Any:
    cache = build_cache(FakeClient(scenario, changed), db_path, **kwargs)
    for i in range(args.rows):
        cache.get(f"hot{i}")
    return cache


class Clock:
    """Stands in for time.time(), so revalidations don't wait on a real ttl."""

    def __init__(self) -> None:
        self.now = time.time()

    def __call__(self) -> float:
        return self.now


def measure_revalidation(
    scenario: Scenario, db_path: str, args: argparse.Namespace, changed: bool
) -> Dict[str, float]:
    from meta_memcache import Key

    keys = scenario.keys("hot_revalidated")
    clock = Clock()
    real_time = time.time
    time.time = clock  # type: ignore[assignment]
    try:

        def revalidator(**overrides: Any) -> Callable[[], None]:
            cache = fresh_cache(
                scenario,
                db_path,
                args,
                changed=changed,
                cache_ttl=REVALIDATE_TTL,
                **overrides,
            )
            client = cache.client
            for key, value in zip(keys, scenario.values):
                store_entry(cache, Key(key), value)
            turn = 0

            def revalidate() -> None:
                # Lands as the entries go stale, so every call revalidates
                nonlocal turn
                if turn % len(keys) == 0:
                    clock.now += REVALIDATE_TTL
                cache.get(keys[turn % len(keys)])
                turn += 1

            calls = client.calls
            for _ in keys:
                revalidate()
            assert client.calls == calls + len(keys), "the lookup didn't revalidate"
            return revalidate

        seconds = time_per_call(revalidator(), args.min_seconds, args.repeats)
        # Never checkpoints, so the log size is what the revalidations wrote
        calls = len(keys) * math.ceil(WAL_CALLS / len(keys))
        wal_bytes = wal_bytes_per_call(
            revalidator(max_wal_bytes=1 << 40), db_path, calls
        )
        return {"us": 1e6 * seconds, "wal_kb": wal_bytes / 1024}
    finally:
        time.time = real_time  # type: ignore[assignment]


class ErrorCounter:
    def __init__(self) -> None:
        self.errors = 0
        self._lock = threading.Lock()

    def init_metrics(self, *args: Any, **kwargs: Any) -> None:
        pass

    def gauge_set(self, *args: Any, **kwargs: Any) -> None:
        pass

    def metric_inc(self, key: str, value: int = 1, labels: Any = None) -> None:
        if key == "errors":
            with self._lock:
                self.errors += value


def measure_threads(
    scenario: Scenario, db_path: str, args: argparse.Namespace
) -> Dict[str, float]:
    # Real clock and a 1s ttl: every key is revalidated about once a second
    # by whichever thread wins the election, while the rest hit
    errors = ErrorCounter()
    cache = fresh_cache(
        scenario, db_path, args, cache_ttl=REVALIDATE_TTL, metrics_collector=errors
    )
    keys = scenario.keys("hot_threaded")
    for key in keys:
        cache.get(key)
    ops = [0] * args.threads
    ready = threading.Barrier(args.threads + 1)
    stop = threading.Event()

    def worker(n: int) -> None:
        get = cycle(cache.get, keys[n:] + keys[:n])
        ready.wait()
        while not stop.is_set():
            get()
            ops[n] += 1

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(len(ops))]
    for thread in threads:
        thread.start()
    ready.wait()
    rates = []
    for _ in range(args.repeats):
        before, start = sum(ops), time.perf_counter()
        time.sleep(args.min_seconds / args.repeats)
        rates.append((sum(ops) - before) / (time.perf_counter() - start))
    stop.set()
    for thread in threads:
        thread.join()
    return {"us": 1e6 / statistics.median(rates), "errors": errors.errors}


def wal_bytes_per_call(fn: Callable[[], Any], db_path: str, calls: int) -> float:
    conn = sqlite3.connect(db_path)
    try:
        busy = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]
        assert not busy, "couldn't truncate the log, a reader is holding it"
        for _ in range(calls):
            fn()
        return os.path.getsize(db_path + "-wal") / calls
    finally:
        conn.close()


def cycle(fn: Callable[[Any], Any], items: List[Any]) -> Callable[[], None]:
    turn = 0

    def call() -> None:
        nonlocal turn
        fn(items[turn % len(items)])
        turn += 1

    return call


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


def flip_last_byte(value: bytes) -> bytes:
    # Differs only at the very end: the worst case for comparing values
    return value[:-1] + bytes([value[-1] ^ 1])


def parse_mix(spec: str, keys: int) -> List[int]:
    if not spec:
        return []
    rng = random.Random(0)
    buckets = [part.split(":") for part in spec.split(",")]
    total = sum(float(weight) for _, weight in buckets)
    sizes = []
    for bucket, weight in buckets:
        lo, _, hi = bucket.partition("-")
        low, high = math.log(int(lo)), math.log(int(hi or lo))
        for _ in range(round(keys * float(weight) / total)):
            sizes.append(round(math.exp(rng.uniform(low, high))))
    rng.shuffle(sizes)
    return sizes


def scenarios(args: argparse.Namespace) -> List[Scenario]:
    values = {size: os.urandom(size) for size in {*args.sizes, *args.mix}}
    result = [Scenario(str(size), [values[size]]) for size in args.sizes]
    if args.mix:
        result.append(Scenario("mix", [values[size] for size in args.mix]))
    return result


def run_local(args: argparse.Namespace) -> List[Dict[str, Any]]:
    from meta_memcache import Key

    py, lite = sys.version.split()[0], sqlite3.sqlite_version
    print(f"  python {py}, sqlite {lite}", file=sys.stderr, flush=True)
    results = []
    with tempfile.TemporaryDirectory(dir=args.db_dir) as tmp:
        # One path, recreated by every measurement: each gets a fresh db
        db_path = f"{tmp}/hot.db"
        for scenario in scenarios(args):
            print(f"  size {scenario.label}...", file=sys.stderr, flush=True)
            hit_keys = scenario.keys("hot")

            def measure(fn: Callable[[], Any]) -> float:
                return 1e6 * time_per_call(fn, args.min_seconds, args.repeats)

            def hits() -> float:
                cache = fresh_cache(scenario, db_path, args)
                for key, value in zip(hit_keys, scenario.values):
                    assert cache.get(key) == value
                return measure(cycle(cache.get, hit_keys))

            def misses() -> float:
                cache = fresh_cache(scenario, db_path, args)
                return measure(lambda: cache.get("cold"))

            def stores() -> float:
                cache = fresh_cache(scenario, db_path, args)
                return measure(
                    cycle(
                        lambda key: store_entry(
                            cache, key, scenario.values[key_index(key.key)]
                        ),
                        [Key(k) for k in hit_keys],
                    )
                )

            results.append(
                {
                    "size": scenario.label,
                    "rows": args.rows,
                    "hit_us": hits(),
                    "miss_us": misses(),
                    **{
                        f"reval_{k}": v
                        for k, v in measure_revalidation(
                            scenario, db_path, args, changed=False
                        ).items()
                    },
                    **{
                        f"reval_changed_{k}": v
                        for k, v in measure_revalidation(
                            scenario, db_path, args, changed=True
                        ).items()
                    },
                    "store_us": stores(),
                    **(
                        {
                            f"mt_{k}": v
                            for k, v in measure_threads(scenario, db_path, args).items()
                        }
                        if args.threads
                        else {}
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
    # Isolated, so every revision gets the same interpreter and the repo's
    # own .venv is left alone
    python = args.python or "{}.{}".format(*sys.version_info)
    cmd = ["uv", "run", "-q", "--isolated", "--project", str(tree), "--python", python]
    cmd += [
        "python",
        str(Path(__file__).resolve()),
        "--json",
        "--sizes",
        ",".join(map(str, args.sizes)),
        "--mix",
        args.mix_spec,
        "--mix-keys",
        str(args.mix_keys),
        "--rows",
        str(args.rows),
        "--min-seconds",
        str(args.min_seconds),
        "--repeats",
        str(args.repeats),
        "--threads",
        str(args.threads),
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
            "reval_us",
            "reval_wal_kb",
            "reval_changed_us",
            "reval_changed_wal_kb",
            "store_us",
            "mt_us",
            "mt_errors",
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
    parser.add_argument(
        "--mix",
        default="100:70,4000-1000000:30",
        help="size:weight or lo-hi:weight buckets for the mix row, empty to skip",
    )
    parser.add_argument("--mix-keys", type=int, default=100)
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument(
        "--min-seconds", type=float, default=1.0, help="time budget per measurement"
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--threads", type=int, default=4, help="for the mt_* columns, 0 to skip"
    )
    parser.add_argument("--db-dir", help="where to put the db, eg: /dev/shm")
    parser.add_argument(
        "--python", help="interpreter for --rev runs, defaults to this one"
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.db_dir and not os.path.isdir(args.db_dir):
        parser.error(f"--db-dir {args.db_dir} does not exist")
    args.mix_spec, args.mix = args.mix, parse_mix(args.mix, args.mix_keys)

    if not args.rev:
        rows = run_local(args)
    else:
        rows = []
        try:
            with tempfile.TemporaryDirectory() as tmp:
                for rev in args.rev:
                    print(f"{rev}:", file=sys.stderr, flush=True)
                    rows += run_rev(rev, args, tmp)
        finally:
            subprocess.run(["git", "-C", str(REPO), "worktree", "prune"])
        labels = [s.label for s in scenarios(args)]
        rows.sort(key=lambda r: (labels.index(r["size"]), args.rev.index(r["rev"])))
    if args.json:
        json.dump(rows, sys.stdout)
    else:
        print_table(rows)


if __name__ == "__main__":
    main()
