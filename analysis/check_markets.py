#!/usr/bin/env python3
"""
Build a mapping from token_id -> question using the existing cache loader.

- Imports load_cache (and CACHE_FILE) from your existing module
- Handles both wrapped and raw cache file formats
- Writes token_to_question.json and prints a short summary
"""

import json
import os
import sys
from typing import Dict, Any

# Update this import if your file name is different (e.g., markets_cache.py)
from fetch_markets import load_cache, CACHE_FILE  # <-- change module name if needed


def _extract_markets_from_obj(obj: Any) -> Dict[str, Any]:
    """
    Accept either:
      - wrapped: {"markets": {...}, "last_cursor": "...", ...}
      - raw:     {"0x...cond_id": {market_obj}, ...}
    Return the markets dict only.
    """
    if isinstance(obj, dict):
        if "markets" in obj and isinstance(obj["markets"], dict):
            return obj["markets"]
        # Heuristic: raw markets look like dict of condition_id -> market dicts
        # We check a few keys on the first value.
        if obj:
            first_val = next(iter(obj.values()))
            if isinstance(first_val, dict) and ("question" in first_val or "tokens" in first_val):
                return obj
    return {}


def _fallback_load_raw_cache(path: str) -> Dict[str, Any]:
    """If load_cache() returns empty, try to read the file directly."""
    try:
        with open(path, "r") as f:
            obj = json.load(f)
        return _extract_markets_from_obj(obj)
    except Exception:
        return {}


def _best_token_id(token: Dict[str, Any]) -> str:
    """Mirror your ID extraction logic across possible fields."""
    return (
        token.get("token_id")
        or token.get("clob_token_id")
        or token.get("clobTokenId")
        or token.get("id")
        or ""
    )


def build_token_to_question_map() -> Dict[str, str]:
    """
    Use load_cache() first; if empty, fall back to directly parsing CACHE_FILE.
    Returns: dict[token_id] = question
    """
    markets_data, _ = load_cache()

    # If markets_data is empty, attempt a direct parse as a fallback
    if not markets_data:
        if os.path.exists(CACHE_FILE):
            markets_data = _fallback_load_raw_cache(CACHE_FILE)

    token_to_question: Dict[str, str] = {}

    for market in markets_data.values():
        if not isinstance(market, dict):
            continue
        question = market.get("question") or ""
        tokens = market.get("tokens") or []

        if not question or not isinstance(tokens, list):
            continue

        for token in tokens:
            if not isinstance(token, dict):
                continue
            tid = _best_token_id(token)
            if tid:
                token_to_question[tid] = question

    return token_to_question


def main():
    mapping = build_token_to_question_map()
    print(mapping['100125813505655528200681223732895453909414094045577213300721684050758066791518'])


if __name__ == "__main__":
    sys.exit(main())
