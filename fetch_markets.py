#!/usr/bin/env python3
"""
Append-only Polymarket markets cache with API compatibility.

Public API preserved:
- get_tradeable_asset_mappings(force_update: bool = False) -> dict
- get_tradeable_markets() -> dict
- extract_asset_mappings() -> dict
- update_markets_cache(full_refresh: bool = False) -> dict
- load_cache_streaming() -> (dict, str)   # compatibility shim (now O(1), no re-read)

Behavior:
- Load JSONL once at process start (or first call) into memory.
- On incremental updates, append only *new* markets to JSONL and update in-memory dict.
- Never re-parse the JSONL again while the process runs.
"""

from __future__ import annotations
import os
import threading
from typing import Dict, Tuple
import requests
from orjson import loads, dumps

CLOB_BASE = "https://clob.polymarket.com"
CACHE_FILE = "markets_cache.jsonl"
CURSOR_FILE = "markets_cursor.txt"

# ---- In-memory state (lazy-loaded) ----
_markets: Dict[str, dict] | None = None
_cursor: str | None = None
_lock = threading.RLock()  # protect _markets/_cursor during updates


# ----------------- Helpers -----------------

def _read_cursor() -> str:
    try:
        with open(CURSOR_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def _write_cursor(c: str) -> None:
    # Persist only meaningful cursors
    if c and c != "LTE=":
        tmp = CURSOR_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(c)
        os.replace(tmp, CURSOR_FILE)


def _load_cache_once() -> None:
    """Load JSONL cache *once* into memory; idempotent."""
    global _markets, _cursor
    if _markets is not None:
        return

    markets: Dict[str, dict] = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                rec = loads(line)
                if rec.get("_type") == "market":
                    cid = rec.pop("condition_id", None)
                    rec.pop("_type", None)
                    if cid:
                        markets[cid] = rec

    _markets = markets
    _cursor = _read_cursor()


def _append_new_markets_to_disk(new_items: list[tuple[str, dict]], cursor_after: str) -> None:
    """Append new markets to JSONL and persist cursor; update in-memory dict too."""
    global _markets, _cursor
    if not new_items:
        # Still persist cursor if it advanced
        if cursor_after and cursor_after != "LTE=":
            _cursor = cursor_after
            _write_cursor(_cursor)
        return

    with open(CACHE_FILE, "a") as out:
        for cid, market in new_items:
            out.write(dumps({"_type": "market", "condition_id": cid, **market}).decode() + "\n")
            _markets[cid] = market  # update in-memory

    if cursor_after and cursor_after != "LTE=":
        _cursor = cursor_after
        _write_cursor(_cursor)


def _fetch_markets_from_cursor(start_cursor: str):
    """Generator yielding (cursor_used, data_page, next_cursor)."""
    cursor = start_cursor or ""
    while True:
        try:
            r = requests.get(f"{CLOB_BASE}/markets",
                             params={"next_cursor": cursor} if cursor else {},
                             timeout=20)
            r.raise_for_status()
            page = r.json()
            data = page.get("data", []) or []
            if not data:
                break
            next_cursor = page.get("next_cursor") or "LTE="
            yield cursor, data, next_cursor
            if next_cursor == "LTE=":
                break
            cursor = next_cursor
        except Exception as e:
            print(f"[fetch_markets] Error: {e}")
            break


# ----------------- Public API -----------------

def update_markets_cache_incremental() -> Dict[str, dict]:
    """Fetch new markets after the current cursor and append only *new* IDs."""
    global _markets, _cursor
    with _lock:
        _load_cache_once()
        assert _markets is not None
        start_cursor = _cursor or ""
        new_items: list[tuple[str, dict]] = []
        final_cursor = start_cursor

        for cursor_used, data, next_cursor in _fetch_markets_from_cursor(start_cursor):
            for m in data:
                cid = m.get("condition_id")
                if cid and cid not in _markets:
                    new_items.append((cid, m))
            if next_cursor != "LTE=":
                final_cursor = next_cursor
            else:
                break

        _append_new_markets_to_disk(new_items, final_cursor)
        if new_items:
            print(f"[markets] +{len(new_items)} new (total={len(_markets)})")
        else:
            print("[markets] No new markets.")
        return _markets


def update_markets_cache(full_refresh: bool = False) -> Dict[str, dict]:
    """Compatibility wrapper with old signature."""
    global _markets, _cursor
    with _lock:
        if full_refresh:
            # Clear on-disk cache & cursor, in-memory too; then refill
            if os.path.exists(CACHE_FILE):
                os.remove(CACHE_FILE)
            if os.path.exists(CURSOR_FILE):
                os.remove(CURSOR_FILE)
            _markets = {}
            _cursor = ""
        # Always do incremental fetch (cheap) to preserve old behavior
        return update_markets_cache_incremental()


def load_cache_streaming() -> Tuple[Dict[str, dict], str]:
    """
    Compatibility shim: previously re-parsed the file.
    Now returns the in-memory dict and cursor (O(1), no disk read).
    """
    with _lock:
        _load_cache_once()
        return _markets, (_cursor or "")


def get_tradeable_markets() -> Dict[str, dict]:
    with _lock:
        _load_cache_once()
        assert _markets is not None
        # No disk I/O; in-memory filter
        return {cid: m for cid, m in _markets.items() if m.get("enable_order_book")}


def extract_asset_mappings() -> dict:
    tradeable = get_tradeable_markets()

    allowed_asset_ids = set()
    asset_to_market: Dict[str, str] = {}
    asset_outcome: Dict[str, str] = {}
    market_to_first_asset: Dict[str, str] = {}

    for condition_id, market in tradeable.items():
        tokens = market.get("tokens", []) or []
        if not tokens:
            continue

        title = condition_id
        first_token_found = False

        for token in tokens:
            token_id = (
                token.get("token_id")
                or token.get("clob_token_id")
                or token.get("clobTokenId")
                or token.get("id")
            )
            if not token_id:
                continue

            outcome = (token.get("outcome") or token.get("name") or "").title()

            allowed_asset_ids.add(token_id)
            asset_to_market[token_id] = title[:80]
            if outcome:
                asset_outcome[token_id] = outcome

            if not first_token_found:
                market_to_first_asset[title] = token_id
                first_token_found = True

    return {
        "allowed_asset_ids": sorted(allowed_asset_ids),
        "asset_to_market": asset_to_market,
        "asset_outcome": asset_outcome,
        "market_to_first_asset": market_to_first_asset,
    }


def get_tradeable_asset_mappings(force_update: bool = False) -> dict:
    """
    Public entry-point used by capture_real_data.py.
    Preserves old semantics: each call performs a cheap incremental update,
    then returns the mapping built from in-memory state.
    """
    with _lock:
        _load_cache_once()
        if force_update:
            update_markets_cache(full_refresh=True)
        else:
            # cheap incremental pass; if no new data, it's basically a no-op
            update_markets_cache_incremental()

        tradeable = get_tradeable_markets()
        mappings = extract_asset_mappings()
        mappings["total_markets"] = len(_markets or {})
        mappings["tradeable_markets"] = len(tradeable)
        return mappings


# ------------- CLI (optional) -------------

if __name__ == "__main__":
    # Warm the cache and do one incremental update (matches old behavior)
    update_markets_cache(full_refresh=False)
    m = get_tradeable_asset_mappings(force_update=False)
    print(f"Total markets: {m['total_markets']}")
    print(f"Tradeable markets: {m['tradeable_markets']}")
    print(f"Asset IDs: {len(m['allowed_asset_ids'])}")
