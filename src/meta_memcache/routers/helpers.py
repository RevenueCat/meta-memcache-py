from typing import Optional

from meta_memcache.protocol import RequestFlags


def _adjust_ttl(ttl: Optional[int], max_ttl: int) -> Optional[int]:
    if ttl is not None and (ttl == 0 or ttl > max_ttl):
        return max_ttl
    return ttl


def adjust_flags_for_max_ttl(
    flags: Optional[RequestFlags],
    max_ttl: int,
) -> Optional[RequestFlags]:
    """
    Override TTLs > than `max_ttl`
    """
    if flags:
        flags.cache_ttl = _adjust_ttl(flags.cache_ttl, max_ttl)
        flags.recache_ttl = _adjust_ttl(flags.recache_ttl, max_ttl)
        flags.vivify_on_miss_ttl = _adjust_ttl(flags.vivify_on_miss_ttl, max_ttl)

    return flags
