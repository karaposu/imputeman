import os
from typing import List, Dict, Any, Optional
# ─────────────────────────────────────────────────────
# Helper – resolve env var with optional override
# ─────────────────────────────────────────────────────
def _resolve(key: str, override: Optional[str]) -> Optional[str]:
    if override is not None:
        return override
    return os.getenv(key)