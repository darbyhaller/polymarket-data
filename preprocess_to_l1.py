#!/usr/bin/env python3
"""
Preprocess L2 order book data to L1 (top-of-book) with **offline, exact ordering**.

Key features
------------
- **External sort by event time** (and optional stream key) for out-of-memory datasets
- **Outage detection** with exact lookahead (no watermarking): gap > --gap-threshold-seconds
- **Per-stream outages** via --key-field (default: asset_id). Use "GLOBAL" for whole firehose
- Reads directories with partitioned layout or loose files; supports gzip inputs
- Writes to a single output file or a RotatingGzipWriter directory (cloud mode)
- Optionally write outages **interleaved** with L1 or to a separate file via --outages-output

Usage examples
--------------
  # Single directory to single file (interleaved outages)
  python preprocess_to_l1_sorted.py /var/data/polymarket l1.jsonl

  # Cloud mode (RotatingGzipWriter directory) + separate outages file
  python preprocess_to_l1_sorted.py /var/data/polymarket --cloud-output /var/data/polymarket/l1 \
      --outages-output /var/data/polymarket/outages.jsonl

  # Per-market outages, 1s gap threshold, chunked sorting
  python preprocess_to_l1_sorted.py /var/data/polymarket l1.jsonl \
      --key-field asset_id --gap-threshold-seconds 1.0 --chunk-max-records 1000000

Notes
-----
- This is a full replacement for the watermark-based streaming detector. Since we operate
  post-hoc, we sort first then scan, so outages are exact and immediate.
- Chunked external sort keeps memory bounded. Adjust chunk sizes with CLI flags.
"""

import argparse
import gzip
import heapq
import json
import os
import sys
import glob
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, Iterator, List, Optional, Tuple, Union

# Import your existing writer utility (unchanged)
from writer import RotatingGzipWriter

# ---------------------------
# Data classes / L1 processor
# ---------------------------
ONE = Decimal("1")

@dataclass
class L1Quote:
    ts_ms: int
    asset_id: str
    market: str
    market_title: str
    outcome: str
    best_bid_price: Optional[str] = None
    best_bid_size: Optional[str] = None
    best_ask_price: Optional[str] = None
    best_ask_size: Optional[str] = None

    def to_dict(self) -> Dict:
        should_flip = self.outcome.lower() in ["down", "no"]
        if should_flip and self.best_bid_price and self.best_ask_price:
            try:
                original_bid = Decimal(self.best_bid_price)
                original_ask = Decimal(self.best_ask_price)
                flipped_bid_price = str(ONE - original_ask)
                flipped_ask_price = str(ONE - original_bid)
                flipped_bid_size = self.best_ask_size
                flipped_ask_size = self.best_bid_size
            except Exception:
                flipped_bid_price = self.best_bid_price
                flipped_ask_price = self.best_ask_price
                flipped_bid_size = self.best_bid_size
                flipped_ask_size = self.best_ask_size
        else:
            flipped_bid_price = self.best_bid_price
            flipped_ask_price = self.best_ask_price
            flipped_bid_size = self.best_bid_size
            flipped_ask_size = self.best_ask_size

        return {
            "ts_ms": self.ts_ms,
            "asset_id": self.asset_id,
            "market": self.market,
            "market_title": self.market_title,
            "outcome": self.outcome,
            "best_bid_price": flipped_bid_price,
            "best_bid_size": flipped_bid_size,
            "best_ask_price": flipped_ask_price,
            "best_ask_size": flipped_ask_size,
            "spread": self._spread(flipped_bid_price, flipped_ask_price),
            "mid_price": self._mid(flipped_bid_price, flipped_ask_price),
        }

    @staticmethod
    def _spread(bid_price: Optional[str], ask_price: Optional[str]) -> Optional[str]:
        if bid_price and ask_price:
            try:
                return str(Decimal(ask_price) - Decimal(bid_price))
            except Exception:
                return None
        return None

    @staticmethod
    def _mid(bid_price: Optional[str], ask_price: Optional[str]) -> Optional[str]:
        if bid_price and ask_price:
            try:
                return str((Decimal(ask_price) + Decimal(bid_price)) / 2)
            except Exception:
                return None
        return None

class L1Processor:
    def __init__(self) -> None:
        self.l1_state: Dict[str, L1Quote] = {}

    @staticmethod
    def _best_levels(bids: List[Dict], asks: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
        best_bid = None
        best_ask = None
        if bids:
            try:
                best_bid = max(bids, key=lambda x: Decimal(x.get("price", "0")))
            except Exception:
                best_bid = bids[0]
        if asks:
            try:
                best_ask = min(asks, key=lambda x: Decimal(x.get("price", "999999")))
            except Exception:
                best_ask = asks[0]
        return best_bid, best_ask

    def process_book(self, ev: Dict) -> Optional[L1Quote]:
        asset_id = ev.get("asset_id")
        if not asset_id:
            return None
        bids = ev.get("bids", [])
        asks = ev.get("asks", [])
        best_bid, best_ask = self._best_levels(bids, asks)
        t = ev.get("timestamp") or ev.get("ts_ms")
        if t is None:
            return None
        ts_ms = int(t)
        q = L1Quote(
            ts_ms=ts_ms,
            asset_id=asset_id,
            market=ev.get("market", ""),
            market_title=ev.get("market_title", ""),
            outcome=ev.get("outcome", ""),
        )
        if best_bid:
            q.best_bid_price = best_bid.get("price")
            q.best_bid_size = best_bid.get("size")
        if best_ask:
            q.best_ask_price = best_ask.get("price")
            q.best_ask_size = best_ask.get("size")
        self.l1_state[asset_id] = q
        return q

    def process_price_change(self, ev: Dict) -> Optional[L1Quote]:
        asset_id = ev.get("asset_id")
        if not asset_id or asset_id not in self.l1_state:
            return None
        changes = ev.get("changes", [])
        q = self.l1_state[asset_id]
        updated = False
        for ch in changes:
            price = ch.get("price")
            side = ch.get("side")  # "buy" or "sell"
            size = ch.get("size")
            if not price or not side:
                continue
            try:
                p = Decimal(price)
                s = Decimal(size) if size else Decimal("0")
                if side == "buy":
                    cur = Decimal(q.best_bid_price) if q.best_bid_price else Decimal("0")
                    if p >= cur:
                        if s > 0:
                            q.best_bid_price = price
                            q.best_bid_size = size
                            updated = True
                        elif p == cur:
                            q.best_bid_price = None
                            q.best_bid_size = None
                            updated = True
                elif side == "sell":
                    cur = Decimal(q.best_ask_price) if q.best_ask_price else Decimal("999999")
                    if p <= cur:
                        if s > 0:
                            q.best_ask_price = price
                            q.best_ask_size = size
                            updated = True
                        elif p == cur:
                            q.best_ask_price = None
                            q.best_ask_size = None
                            updated = True
            except Exception:
                continue
        if updated:
            t = ev.get("timestamp") or ev.get("ts_ms")
            if t is not None:
                q.ts_ms = int(t)
            return q
        return None

    def process_event(self, ev: Dict) -> Optional[L1Quote]:
        et = ev.get("event_type")
        if et == "book":
            return self.process_book(ev)
        elif et == "price_change":
            return self.process_price_change(ev)
        else:
            return None

# --------------------
# IO utilities
# --------------------

def open_file_smart(path: str):
    return gzip.open(path, "rt", encoding="utf-8") if path.endswith(".gz") else open(path, "r", encoding="utf-8")


def discover_input_files(input_path: str) -> List[str]:
    files: List[str] = []
    if input_path.endswith('.jsonl') or input_path.endswith('.jsonl.gz'):
        if os.path.exists(input_path):
            return [input_path]
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if os.path.isdir(input_path):
        pattern = os.path.join(input_path, "year=*", "month=*", "day=*", "hour=*", "events-*.jsonl.gz")
        files.extend(glob.glob(pattern))
        for ext in ["*.jsonl", "*.jsonl.gz"]:
            files.extend(glob.glob(os.path.join(input_path, ext)))
    files.sort()
    if not files:
        raise FileNotFoundError(f"No input files found in: {input_path}")
    return files

# --------------------
# External sort (chunk → k-way merge)
# --------------------

ChunkRecord = Tuple[str, int, str]  # (key, ts_ms, raw_json_line)


def extract_key_ts(line: str, key_field: Optional[str]) -> Optional[Tuple[str, int]]:
    try:
        ev = json.loads(line)
    except json.JSONDecodeError:
        return None
    t = ev.get("timestamp") or ev.get("ts_ms")
    if t is None:
        return None
    k = ev.get(key_field) if key_field else "GLOBAL"
    try:
        ts = int(t)
    except Exception:
        return None
    return (str(k) if k is not None else "GLOBAL", ts)


def write_sorted_chunk(tmpdir: str, chunk: List[ChunkRecord], idx: int) -> str:
    chunk.sort(key=lambda r: (r[0], r[1]))
    path = os.path.join(tmpdir, f"chunk_{idx:05d}.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for k, ts, raw in chunk:
            # store envelope as JSON for safety
            f.write(json.dumps({"k": k, "ts": ts, "raw": raw}) + "\n")
    return path


def make_chunks(input_files: List[str], key_field: Optional[str], chunk_max_records: int, chunk_max_bytes: int) -> List[str]:
    tmpdir = tempfile.mkdtemp(prefix="l1sort_")
    chunk: List[ChunkRecord] = []
    chunks: List[str] = []
    recs = 0
    bytes_acc = 0
    chunk_idx = 0

    for path in input_files:
        with open_file_smart(path) as f:
            for line in f:
                if not line or line == "\n":
                    continue
                key_ts = extract_key_ts(line, key_field)
                if key_ts is None:
                    continue
                k, ts = key_ts
                chunk.append((k, ts, line.rstrip('\n')))
                recs += 1
                bytes_acc += len(line)
                if recs >= chunk_max_records or bytes_acc >= chunk_max_bytes:
                    chunks.append(write_sorted_chunk(tmpdir, chunk, chunk_idx))
                    chunk_idx += 1
                    chunk.clear()
                    recs = 0
                    bytes_acc = 0
    if chunk:
        chunks.append(write_sorted_chunk(tmpdir, chunk, chunk_idx))
    return chunks


@dataclass
class HeapItem:
    k: str
    ts: int
    raw: str
    src_idx: int  # which chunk file

    def __lt__(self, other: "HeapItem") -> bool:
        return (self.k, self.ts) < (other.k, other.ts)


def merge_and_process(
    chunk_paths: List[str],
    processor: L1Processor,
    gap_threshold_s: float,
    writer_main: Union[RotatingGzipWriter, str],
    writer_outages: Optional[Union[RotatingGzipWriter, str]],
    interleave_outages: bool,
    verbose: bool = False,
) -> Tuple[int, int, int]:
    """K-way merge the sorted chunks; emit L1 updates and outages in order.
    Returns: (events_read, l1_updates, outages)
    """
    # Prepare output handles
    main_out_file = None
    outages_out_file = None
    if isinstance(writer_main, str):
        main_out_file = open(writer_main, "w", encoding="utf-8")
    if writer_outages is not None and isinstance(writer_outages, str):
        outages_out_file = open(writer_outages, "w", encoding="utf-8")

    def write_record(rec: Dict, is_outage: bool = False):
        j = json.dumps(rec)
        if is_outage and writer_outages is not None and not interleave_outages:
            if isinstance(writer_outages, str):
                assert outages_out_file is not None
                outages_out_file.write(j + "\n")
            else:
                writer_outages.write(rec)
        else:
            if isinstance(writer_main, str):
                assert main_out_file is not None
                main_out_file.write(j + "\n")
            else:
                writer_main.write(rec)

    # Open all chunk files
    files = [open(p, "r", encoding="utf-8") for p in chunk_paths]

    heap: List[HeapItem] = []
    for i, fh in enumerate(files):
        line = fh.readline()
        if not line:
            continue
        env = json.loads(line)
        heapq.heappush(heap, HeapItem(env["k"], int(env["ts"]), env["raw"], i))

    prev_ts_by_key: Dict[str, int] = {}
    events_read = 0
    l1_updates = 0
    outages = 0
    ms = 1000.0

    try:
        while heap:
            item = heapq.heappop(heap)
            k, ts, raw, idx = item.k, item.ts, item.raw, item.src_idx
            # refill from same file
            nxt = files[idx].readline()
            if nxt:
                env = json.loads(nxt)
                heapq.heappush(heap, HeapItem(env["k"], int(env["ts"]), env["raw"], idx))

            # Outage detection (exact, per key)
            if k in prev_ts_by_key:
                gap_s = (ts - prev_ts_by_key[k]) / ms
                if gap_s > gap_threshold_s:
                    outage_rec = {
                        "ts_ms": prev_ts_by_key[k],  # pin at start of silence
                        "event_type": "no_network_event",
                        "gap_duration_ms": int(gap_s * 1000),
                        "outage_start": datetime.fromtimestamp(prev_ts_by_key[k]/1000.0, tz=timezone.utc).isoformat(),
                        "outage_end": datetime.fromtimestamp(ts/1000.0, tz=timezone.utc).isoformat(),
                        "stream_key": k,
                        "message": f"Network outage detected - no events for {gap_s:.3f} seconds",
                    }
                    write_record(outage_rec, is_outage=True)
                    outages += 1
                    if verbose:
                        print(f"OUTAGE {k}: {outage_rec['outage_start']} → {outage_rec['outage_end']} ({gap_s:.3f}s)")
            prev_ts_by_key[k] = ts

            # L1 processing in exact event-time order
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                continue
            events_read += 1
            l1 = processor.process_event(ev)
            if l1:
                write_record(l1.to_dict(), is_outage=False)
                l1_updates += 1
    finally:
        for fh in files:
            try:
                fh.close()
            except Exception:
                pass
        if main_out_file:
            main_out_file.close()
        if outages_out_file:
            outages_out_file.close()

    return events_read, l1_updates, outages

# --------------------
# Main
# --------------------

def main():
    parser = argparse.ArgumentParser(
        description="Preprocess L2 → L1 with external sort and exact outage detection",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('input', help='Input file or directory path')
    parser.add_argument('output', nargs='?', help='Output file (omit if using --cloud-output)')
    parser.add_argument('--cloud-output', metavar='DIR', help='Write via RotatingGzipWriter to directory')
    parser.add_argument('--key-field', default='asset_id', help='Stream key field; leave empty for GLOBAL')
    parser.add_argument('--gap-threshold-seconds', type=float, default=1.0, help='Gap threshold for outages')
    parser.add_argument('--outages-output', help='Separate outages output file; if omitted, interleave into main output')
    parser.add_argument('--chunk-max-records', type=int, default=500_000, help='Max records per chunk before sort spill')
    parser.add_argument('--chunk-max-mb', type=int, default=256, help='Approx MB per chunk before sort spill')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging (prints outages)')

    args = parser.parse_args()

    if not args.cloud_output and not args.output:
        parser.error("Must specify either output file or --cloud-output")
    if args.cloud_output and args.output:
        parser.error("Cannot specify both output file and --cloud-output")

    key_field = args.key_field if args.key_field else None

    # Discover inputs
    input_files = discover_input_files(args.input)
    print(f"Discovered {len(input_files)} input files")

    # Build chunks
    chunk_max_bytes = args.chunk_max_mb * 1024 * 1024
    print(f"Chunking with limits: records={args.chunk_max_records}, bytes≈{args.chunk_max_mb}MB")
    chunks = make_chunks(input_files, key_field, args.chunk_max_records, chunk_max_bytes)
    print(f"Created {len(chunks)} sorted chunk(s)")

    # Prepare writers
    if args.cloud_output:
        os.makedirs(args.cloud_output, exist_ok=True)
        main_writer: Union[RotatingGzipWriter, str] = RotatingGzipWriter(args.cloud_output)
        outages_writer: Optional[Union[RotatingGzipWriter, str]] = None
        if args.outages_output:
            # When using cloud writer for main, write outages to a flat file unless you add another writer
            outages_writer = args.outages_output
    else:
        main_writer = args.output  # flat file
        outages_writer = args.outages_output if args.outages_output else None

    # Merge + process
    processor = L1Processor()
    interleave = args.outages_output is None
    events_read, l1_updates, outages = merge_and_process(
        chunks,
        processor,
        args.gap_threshold_seconds,
        main_writer,
        outages_writer,
        interleave_outages=interleave,
        verbose=args.verbose,
    )

    # Stats
    print("Batch preprocessing complete!")
    print(f"Files processed: {len(input_files)}")
    print(f"Total events read: {events_read}")
    print(f"L1 updates written: {l1_updates}")
    print(f"Outage markers written: {outages} ({'interleaved' if interleave else 'separate file'})")


if __name__ == "__main__":
    main()
