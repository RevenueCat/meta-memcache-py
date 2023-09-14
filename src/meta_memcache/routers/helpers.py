from typing import Dict, Optional

from meta_memcache.protocol import IntFlag


def adjust_int_flags_for_max_ttl(
    int_flags: Optional[Dict[IntFlag, int]],
    max_ttl: int,
) -> Optional[Dict[IntFlag, int]]:
    """
    Override TTLs > than `max_ttl`
    """
    if int_flags:
        for flag in (
            IntFlag.CACHE_TTL,
            IntFlag.RECACHE_TTL,
            IntFlag.MISS_LEASE_TTL,
        ):
            ttl = int_flags.get(flag)
            if ttl is not None and (ttl == 0 or ttl > max_ttl):
                int_flags[flag] = max_ttl

    return int_flags
