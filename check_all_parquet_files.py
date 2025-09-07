#!/usr/bin/env python3
"""
Check all parquet files for corruption without removing anything.
Reports detailed statistics on file health.
"""

import os
import glob
from typing import List, Tuple, Dict

def check_parquet_file(filepath: str) -> Tuple[bool, str, int]:
    """Check if a parquet file is valid by verifying magic bytes."""
    try:
        file_size = os.path.getsize(filepath)
        with open(filepath, 'rb') as f:
            # Check header
            header = f.read(4)
            if header != b'PAR1':
                return False, f"Invalid header: {header.hex()} (expected PAR1)", file_size
            
            # Check footer
            f.seek(-4, 2)
            footer = f.read()
            if footer != b'PAR1':
                return False, f"Invalid footer: {footer.hex()} (expected PAR1)", file_size
            
        return True, "Valid", file_size
    except Exception as e:
        return False, f"Error reading file: {e}", 0

def find_all_parquet_files(root: str) -> List[str]:
    """Find all parquet files in the directory tree."""
    files = []
    for root_path, dirs, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith('.parquet'):
                files.append(os.path.join(root_path, filename))
    return sorted(files)

def main():
    root_dir = "hour=19"
    
    if not os.path.exists(root_dir):
        print(f"Directory {root_dir} not found!")
        return
    
    print(f"Scanning all parquet files in {root_dir}...")
    parquet_files = find_all_parquet_files(root_dir)
    
    if not parquet_files:
        print("No parquet files found!")
        return
    
    print(f"Found {len(parquet_files)} parquet files")
    print("=" * 80)
    
    # Statistics
    stats = {
        'valid': 0,
        'corrupted': 0,
        'by_event_type': {},
        'corrupted_files': [],
        'valid_files': [],
        'total_size': 0,
        'corrupted_size': 0
    }
    
    # Check each file
    for filepath in parquet_files:
        is_valid, message, file_size = check_parquet_file(filepath)
        
        # Extract event type from path
        path_parts = filepath.split(os.sep)
        event_type = "unknown"
        for part in path_parts:
            if part.startswith("event_type="):
                event_type = part.replace("event_type=", "")
                break
        
        if event_type not in stats['by_event_type']:
            stats['by_event_type'][event_type] = {'valid': 0, 'corrupted': 0, 'size': 0}
        
        stats['total_size'] += file_size
        stats['by_event_type'][event_type]['size'] += file_size
        
        if is_valid:
            stats['valid'] += 1
            stats['by_event_type'][event_type]['valid'] += 1
            stats['valid_files'].append(filepath)
            print(f"✓ VALID:     {filepath} ({file_size:,} bytes)")
        else:
            stats['corrupted'] += 1
            stats['by_event_type'][event_type]['corrupted'] += 1
            stats['corrupted_files'].append((filepath, message))
            stats['corrupted_size'] += file_size
            print(f"✗ CORRUPTED: {filepath} ({file_size:,} bytes) - {message}")
    
    print("\n" + "=" * 80)
    print("SUMMARY:")
    print(f"Total files: {len(parquet_files)}")
    print(f"Valid files: {stats['valid']} ({stats['valid']/len(parquet_files)*100:.1f}%)")
    print(f"Corrupted files: {stats['corrupted']} ({stats['corrupted']/len(parquet_files)*100:.1f}%)")
    print(f"Total size: {stats['total_size']:,} bytes ({stats['total_size']/1024/1024:.1f} MB)")
    print(f"Corrupted size: {stats['corrupted_size']:,} bytes ({stats['corrupted_size']/1024/1024:.1f} MB)")
    
    print("\nBY EVENT TYPE:")
    for event_type, data in stats['by_event_type'].items():
        total = data['valid'] + data['corrupted']
        corrupt_pct = data['corrupted'] / total * 100 if total > 0 else 0
        size_mb = data['size'] / 1024 / 1024
        print(f"  {event_type}: {data['valid']} valid, {data['corrupted']} corrupted ({corrupt_pct:.1f}% corrupt), {size_mb:.1f} MB")
    
    if stats['corrupted_files']:
        print(f"\nCORRUPTED FILES ({len(stats['corrupted_files'])}):")
        for filepath, reason in stats['corrupted_files']:
            print(f"  {filepath}: {reason}")

if __name__ == "__main__":
    main()