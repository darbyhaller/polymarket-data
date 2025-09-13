#!/usr/bin/env python3
"""
High-performance parquet writer for Polymarket event data.
Partitioned by event_type with optimized schemas for each event type.
"""

from datetime import timedelta
import os
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from collections import defaultdict, deque
import time
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULTS = {
    "compression": os.getenv("PM_COMPRESSION", "zstd"),
    "batch_size": int(os.getenv("PM_BATCH_SIZE", "200000")),     # events buffered per type before flush
    "row_group_size": int(os.getenv("PM_ROW_GROUP_SIZE", "200000")),  # rows per row-group
    "rows_per_file": int(os.getenv("PM_ROWS_PER_FILE", "1000000")) # rotate file around this many rows
}

def price_to_int(price_str: str) -> int:
    """Convert price string to int32 integer (multiply by 10000)."""
    return int(float(price_str) * 10_000)

size_to_int = price_to_int

class EventTypeParquetWriter:
    """
    High-performance parquet writer partitioned by event_type and hour.
    
    Features:
    - Event-type specific schemas for optimal compression
    - Automatic partitioning by event_type/year/month/day/hour  
    - Batched writes for performance
    - Thread-safe operation
    - Memory-efficient buffering with size limits
    - Optimized numeric encoding: prices as int32, sizes as int64
    """
    
    def __init__(
        self,
        root: str,
        batch_size: int = DEFAULTS["batch_size"],
        rotate_mb: int = 256,
        compression: str = DEFAULTS["compression"],
        row_group_size: int = DEFAULTS["row_group_size"],
        rows_per_file: int = DEFAULTS["rows_per_file"],
    ):
        self.root = root
        self.batch_size = batch_size
        self.rotate_bytes = rotate_mb * 1024 * 1024
        self.compression = compression
        self.row_group_size = row_group_size
        self.rows_per_file = rows_per_file
        
        self.lock = threading.Lock()
        
        # Buffers for each event type - using deque for efficient append/popleft
        self.buffers: Dict[str, deque] = defaultdict(deque)
        self.buffer_sizes: Dict[str, int] = defaultdict(int)
        
        # Track current files and open writers
        self.current_files: Dict[str, str] = {}
        self.current_hour: Dict[str, datetime] = {}
        self.file_sequences: Dict[str, int] = defaultdict(int)
        self.writers: Dict[str, pq.ParquetWriter] = {}
        self.rows_written: Dict[str, int] = defaultdict(int)
        
        os.makedirs(root, exist_ok=True)
        
        # Define schemas for each event type
        self._init_schemas()
    
    def _init_schemas(self):
        """Initialize optimized PyArrow schemas for each event type.
        
        Numeric encoding for optimal performance:
        - Prices: multiply by 10,000, store as int32 (range [0,1] -> [0,10000])
        - Sizes: multiply by 10,000, store as int64 (large orders possible)
        - Timestamps: convert to int64
        """
        
        # Common base schema
        base_fields = [
            pa.field("recv_ts_ms", pa.int64()),
            pa.field("asset_id", pa.string()),
            pa.field("market", pa.string()),
            pa.field("outcome", pa.string()),
            pa.field("timestamp", pa.int64()),  # Convert timestamp strings to int64
        ]
        
        # Order summary schema for bids/asks
        order_summary_schema = pa.struct([
            pa.field("price", pa.int32()),  # stored as price * 10000
            pa.field("size", pa.int64())    # stored as size * 10000
        ])
        
        # Price change schema
        price_change_schema = pa.struct([
            pa.field("price", pa.int32()),  # stored as price * 10000
            pa.field("side", pa.string()),
            pa.field("size", pa.int64())    # stored as size * 10000
        ])
        
        self.schemas = {
            "book": pa.schema(base_fields + [
                pa.field("bids", pa.list_(order_summary_schema)),
                pa.field("asks", pa.list_(order_summary_schema))
            ]),
            
            "price_change": pa.schema(base_fields + [
                pa.field("changes", pa.list_(price_change_schema))
            ]),
            
            "tick_size_change": pa.schema(base_fields + [
                pa.field("old_tick_size", pa.int32()),  # stored as tick_size * 10000
                pa.field("new_tick_size", pa.int32())   # stored as tick_size * 10000
            ]),
            
            "last_trade_price": pa.schema(base_fields + [
                pa.field("price", pa.int32()),      # stored as price * 10000
                pa.field("size", pa.int64()),       # stored as size * 10000
                pa.field("side", pa.string()),
                pa.field("fee_rate_bps", pa.int32())  # stored as bps * 100 (0.01 bps resolution)
            ])
        }
    
    def _open_writer(self, file_key: str, schema: pa.Schema, path: str):
        """Open a new ParquetWriter for the given file key with .inprogress pattern."""
        self._close_writer(file_key)  # just in case
        
        # Use .inprogress pattern for atomic writes
        tmp_path = path + ".inprogress"
        self.writers[file_key] = pq.ParquetWriter(
            tmp_path,
            schema=schema,
            compression=self.compression,
            use_dictionary=True,
            write_statistics=True,
        )
        self.rows_written[file_key] = 0
        # Store final path separately for atomic rename on close
        self.current_files[file_key] = path

    def _close_writer(self, file_key: str):
        """Close the ParquetWriter and atomically rename from .inprogress to final path."""
        w = self.writers.pop(file_key, None)
        if w is not None:
            try:
                w.close()
            except Exception:
                pass
            
            # Atomic rename from .inprogress to final path
            final_path = self.current_files.get(file_key)
            if final_path:
                tmp_path = final_path + ".inprogress"
                try:
                    os.replace(tmp_path, final_path)
                except Exception as e:
                    print(f"Atomic rename failed for {tmp_path} -> {final_path}: {e}")

    def close_completed_hours(self, grace_minutes=15):
        """Close writers that are older than current hour minus grace period.
        
        This prevents late events from causing file collisions by keeping writers
        open for a grace period after their hour boundary.
        """
        
        now = datetime.fromtimestamp(time.time(), timezone.utc)
        # Close writers for hours that ended more than grace_minutes ago
        cutoff_time = now.replace(minute=0, second=0, microsecond=0) - timedelta(minutes=grace_minutes)
        
        to_close = []
        for key, hour_dt in list(self.current_hour.items()):
            if hour_dt < cutoff_time:
                to_close.append(key)
        
        if to_close:
            print(f"Closing {len(to_close)} writers older than {cutoff_time} (grace period: {grace_minutes}min)")
        
        for key in to_close:
            self._close_writer(key)
            self.current_hour.pop(key, None)
            self.current_files.pop(key, None)
            self.rows_written.pop(key, None)
    
    def _partition_path(self, event_type: str, dt: datetime) -> str:
        """Generate partition path for event_type and datetime."""
        y, m, d, h = dt.strftime("%Y %m %d %H").split()
        return os.path.join(
            self.root,
            f"event_type={event_type}",
            f"year={y}",
            f"month={m}",
            f"day={d}",
            f"hour={h}"
        )
    
    def _file_path(self, event_type: str, dt: datetime) -> str:
        """Generate full file path including sequence number."""
        partition_dir = self._partition_path(event_type, dt)
        os.makedirs(partition_dir, exist_ok=True)
        
        seq = self.file_sequences[f"{event_type}_{dt}"]
        filename = f"events-{seq:03d}.parquet"
        return os.path.join(partition_dir, filename)
    
    def _normalize_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize event data to match schema expectations and convert to optimal types."""
        normalized = event.copy()
        
        # Ensure all fields are present with proper defaults
        event_type = event.get("event_type", "unknown")
        schema = self.schemas.get(event_type)
        
        if not schema:
            return normalized
        
        # Convert timestamps to integers
        normalized["timestamp"] = int(normalized["timestamp"])
        
        # Handle legacy field names (bids/asks vs buys/sells)
        if event_type == "book":
            if "buys" in normalized and "bids" not in normalized:
                normalized["bids"] = normalized.pop("buys")
            if "sells" in normalized and "asks" not in normalized:
                normalized["asks"] = normalized.pop("sells")
            
            # Convert bid/ask prices and sizes to integers
            for level_type in ["bids", "asks"]:
                if level_type in normalized and normalized[level_type]:
                    converted_levels = []
                    for level in normalized[level_type]:
                        if isinstance(level, dict):
                            converted_levels.append({
                                "price": price_to_int(level.get("price", "0")),
                                "size": size_to_int(level.get("size", "0"))
                            })
                    normalized[level_type] = converted_levels
        
        elif event_type == "price_change":
            # Convert price change data
            if "changes" in normalized and normalized["changes"]:
                converted_changes = []
                for change in normalized["changes"]:
                    if isinstance(change, dict):
                        converted_changes.append({
                            "price": price_to_int(change.get("price", "0")),
                            "side": change.get("side", ""),
                            "size": size_to_int(change.get("size", "0"))
                        })
                normalized["changes"] = converted_changes
        
        elif event_type == "tick_size_change":
            # Convert tick sizes
            normalized["old_tick_size"] = price_to_int(normalized.get("old_tick_size", "0"))
            normalized["new_tick_size"] = price_to_int(normalized.get("new_tick_size", "0"))
        
        elif event_type == "last_trade_price":
            # Convert trade price and size
            normalized["price"] = price_to_int(normalized.get("price", "0"))
            normalized["size"] = size_to_int(normalized.get("size", "0"))
            # Convert fee_rate_bps (multiply by 100 for 0.01 bps resolution)
            try:
                fee_bps = float(normalized.get("fee_rate_bps", "0")) * 100
                normalized["fee_rate_bps"] = int(fee_bps)
            except (ValueError, TypeError):
                normalized["fee_rate_bps"] = 0
        
        # Ensure all required fields are present
        for field in schema:
            if field.name not in normalized:
                # Set appropriate default based on field type
                if field.type in [pa.int32(), pa.int64()]:
                    normalized[field.name] = 0
                elif field.type == pa.string():
                    normalized[field.name] = ""
                else:
                    normalized[field.name] = None
        
        return normalized

    
    def write(self, event: Dict[str, Any]):
        """Write a single event. Thread-safe and batched."""
        event_type = event.get("event_type", "unknown")
        
        # Skip unknown event types
        if event_type not in self.schemas:
            return
        
        normalized_event = self._normalize_event(event)
        
        with self.lock:
            # Add to buffer
            self.buffers[event_type].append(normalized_event)
            
            # Check if we should flush this event type
            should_flush = len(self.buffers[event_type]) >= self.batch_size
            
            if should_flush:
                self._flush_event_type(event_type)
    
    def _flush_event_type(self, event_type: str):
        """Flush buffered events for a specific event type. Must hold lock."""
        if not self.buffers[event_type]:
            return
        
        # Convert buffer to list and clear
        events = list(self.buffers[event_type])
        self.buffers[event_type].clear()
        self.buffer_sizes[event_type] = 0
        
        # Group events by hour for proper partitioning
        hourly_groups = defaultdict(list)
        for event in events:
            # Use recv_ts_ms for partitioning
            recv_ts_ms = event.get("recv_ts_ms", int(time.time() * 1000))
            dt = datetime.fromtimestamp(recv_ts_ms / 1000, timezone.utc)
            hour_key = dt.replace(minute=0, second=0, microsecond=0)
            hourly_groups[hour_key].append(event)
        
        # Write each hourly group
        for hour_dt, hour_events in hourly_groups.items():
            self._write_batch(event_type, hour_dt, hour_events)
    
    def _write_batch(self, event_type: str, hour_dt: datetime, events: List[Dict[str, Any]]):
        """Write a batch of events to parquet. Must hold lock."""
        if not events:
            return
        
        try:
            file_key = f"{event_type}_{hour_dt}"
            schema = self.schemas[event_type]

            # REMOVED: Proactive closing of older hours - unsafe for late events
            # Instead, rely on grace period in close_completed_hours()

            rotate = False
            # rotate by rows or when hour changes (hour change is handled by file_key)
            if file_key in self.writers:
                if self.rows_written[file_key] >= self.rows_per_file:
                    rotate = True

            if rotate:
                self.file_sequences[file_key] += 1
                self._close_writer(file_key)

            # Ensure a writer exists for this file_key
            if file_key not in self.writers:
                path = self._file_path(event_type, hour_dt)
                self.current_hour[file_key] = hour_dt
                self._open_writer(file_key, schema, path)

            # Write this batch as a new row group with optimal row group size
            events.sort(key=lambda e: (e.get("asset_id", ""), e.get("timestamp", 0)))
            table = pa.Table.from_pylist(events, schema=schema)
            self.writers[file_key].write_table(table)
            self.rows_written[file_key] += table.num_rows
            
        except Exception as e:
            # Log error but don't crash the writer
            print(f"Error writing parquet batch for {event_type}: {e}")
    
    def flush(self):
        """Flush all buffered events."""
        with self.lock:
            for event_type in list(self.buffers.keys()):
                self._flush_event_type(event_type)
            self.close_completed_hours()
    
    def close(self):
        """Flush all buffers and close all open Parquet writers."""
        self.flush()
        for key in list(self.writers.keys()):
            self._close_writer(key)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Global writer instance (will be initialized by the main script)
writer: Optional[EventTypeParquetWriter] = None


def write_event(event: Dict[str, Any]):
    """Write a single event using the global writer instance."""
    global writer
    if writer is not None:
        writer.write(event)


def init_writer(**overrides):
    """Initialize the global parquet writer with sane unified defaults."""
    global writer
    params = {**DEFAULTS, **overrides}  # merge overrides into defaults
    writer = EventTypeParquetWriter(**params)

    # Guardrail check (only once)
    if writer.batch_size < writer.row_group_size // 20:
        print(f"[parquet-writer WARNING] batch_size={writer.batch_size} is very small "
              f"vs row_group_size={writer.row_group_size}. Expect many tiny row groups.")
    return writer


def close_writer():
    """Close the global writer."""
    global writer
    writer.close()