import logging
from typing import Callable, Dict, List, NamedTuple, Optional

from meta_memcache.protocol import Key, MemcacheResponse, MetaCommand, RequestFlags

_log: logging.Logger = logging.getLogger(__name__)


class CacheStats(NamedTuple):
    """
    What an operation sent to a server did, handed to the executor's
    stats_callback once it is over.
    """

    command: MetaCommand
    keys: List[Key]
    # Bytes of the values read, as the server sent them.
    size: int
    server: str
    flags: Optional[RequestFlags]
    # Missing when the operation raised. A failure reported instead of raised
    # (raise_on_server_error=False) is here, as an error marker response.
    responses: Optional[Dict[Key, MemcacheResponse]]
    start_time_ns: int
    duration_ns: int
    error: Optional[Exception]
    # How many operations this one stands for, as the stats_sampler said.
    # 1 without a sampler.
    weight: int


class HotCacheStats(NamedTuple):
    """
    The keys a hot cache read served itself, without asking the server,
    handed to the hot cache's stats_callback. The keys it fetches are
    reported by the executor of the client it wraps, as CacheStats.
    """

    keys: List[Key]
    # Bytes the server sent for the values, when they were cached.
    size: int
    # How many reads this one stands for, as the stats_sampler said.
    # 1 without a sampler.
    weight: int


# Called inline after every operation. Stats are best effort: whatever a
# callback raises is logged and ignored, it never fails the operation.
StatsCallback = Callable[[CacheStats], None]
HotCacheStatsCallback = Callable[[HotCacheStats], None]
# Called before every operation, when there is a callback, to decide whether
# to report it: None skips it, and costs nothing else. Otherwise, the weight
# it is reported with, eg: 100 when sampling 1% of them.
StatsSampler = Callable[[], Optional[int]]


def sample_weight(sampler: Optional[StatsSampler]) -> Optional[int]:
    """The weight to report an operation with, or None to skip it."""
    if sampler is None:
        return 1
    try:
        return sampler()
    except Exception:
        # Same as a failing callback: the operation goes on, unreported.
        _log.warning("Error sampling cache stats", exc_info=True)
        return None
