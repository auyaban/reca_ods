from __future__ import annotations

import time


def ttl_bucket(ttl_seconds: int) -> int:
    safe_ttl = max(1, int(ttl_seconds))
    return int(time.time() // safe_ttl)
