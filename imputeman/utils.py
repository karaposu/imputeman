import os
from typing import List, Dict, Any, Optional
# ─────────────────────────────────────────────────────
# Helper – resolve env var with optional override
# ─────────────────────────────────────────────────────
def _resolve(key: str, override: Optional[str]) -> Optional[str]:
    if override is not None:
        return override
    return os.getenv(key)


def load_html(path: str) -> str:
    """Read a local HTML file and return its contents as a string (UTF-8)."""
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()