#!/usr/bin/env python3
"""
Script to check which parquet files have dictionary-encoded strings vs plain strings.
This helps identify schema compatibility issues when reading multiple files together.
"""

import os
import sys
from glob import glob
from collections import defaultdict, Counter
import pyarrow.parquet as pq
import pyarrow as pa


def analyze_field_encoding(field_type):
    """Analyze a PyArrow field type and return encoding info."""
    if pa.types.is_dictionary(field_type):
        value_type = field_type.value_type
        return f"dictionary<{value_type}>"
    elif pa.types.is_string(field_type):
        return "string"
    elif pa.types.is_list(field_type):
        return f"list<{analyze_field_encoding(field_type.value_type)}>"
    elif pa.types.is_struct(field_type):
        struct_fields = []
        for i in range(field_type.num_fields):
            child_field = field_type.field(i)
            child_encoding = analyze_field_encoding(child_field.type)
            struct_fields.append(f"{child_field.name}:{child_encoding}")
        return f"struct<{', '.join(struct_fields)}>"
    else:
        return str(field_type)


def check_file_schema(file_path):
    """Check the schema of a single parquet file."""
    try:
        # Read just the schema without loading data
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema.to_arrow_schema()
        
        field_encodings = {}
        for field in schema:
            field_encodings[field.name] = analyze_field_encoding(field.type)
        
        return {
            'success': True,
            'schema': field_encodings,
            'num_row_groups': parquet_file.num_row_groups,
            'num_rows': parquet_file.metadata.num_rows if parquet_file.metadata else 0
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'schema': {},
            'num_row_groups': 0,
            'num_rows': 0
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Check schema encoding in parquet files")
    parser.add_argument("--root", default="parquets", help="Root directory to scan")
    parser.add_argument("--event-type", help="Specific event type to check (e.g., 'book', 'price_change')")
    parser.add_argument("--detailed", action="store_true", help="Show detailed per-file results")
    parser.add_argument("--field", help="Focus on a specific field (e.g., 'event_type')")
    args = parser.parse_args()
    
    # Find all parquet files
    if args.event_type:
        pattern = os.path.join(args.root, f"event_type={args.event_type}", "**", "*.parquet")
    else:
        pattern = os.path.join(args.root, "**", "*.parquet")
    
    files = [f for f in glob(pattern, recursive=True) if not f.endswith('.inprogress')]
    files.sort()
    
    if not files:
        print(f"No parquet files found in {args.root}")
        return
    
    print(f"Found {len(files)} parquet files")
    print()
    
    # Track schema variations
    schema_variations = defaultdict(list)  # schema_signature -> [file_paths]
    field_encodings = defaultdict(Counter)  # field_name -> Counter({encoding: count})
    errors = []
    
    total_files = len(files)
    total_rows = 0
    
    for i, file_path in enumerate(files):
        if i % 50 == 0:
            print(f"Processing {i+1}/{total_files}...", file=sys.stderr)
        
        result = check_file_schema(file_path)
        
        if not result['success']:
            errors.append((file_path, result['error']))
            continue
            
        total_rows += result['num_rows']
        
        # Create a signature for this schema
        schema_items = sorted(result['schema'].items())
        if args.field:
            # Focus on specific field
            schema_items = [(k, v) for k, v in schema_items if k == args.field]
        
        schema_signature = tuple(schema_items)
        schema_variations[schema_signature].append(file_path)
        
        # Count field encodings
        for field_name, encoding in result['schema'].items():
            field_encodings[field_name][encoding] += 1
        
        if args.detailed:
            print(f"\n{file_path}:")
            print(f"  Rows: {result['num_rows']:,}")
            print(f"  Row groups: {result['num_row_groups']}")
            if args.field and args.field in result['schema']:
                print(f"  {args.field}: {result['schema'][args.field]}")
            else:
                for field_name, encoding in sorted(result['schema'].items()):
                    print(f"  {field_name}: {encoding}")
    
    print(f"\nAnalyzed {len(files)} files, {total_rows:,} total rows")
    
    # Report errors
    if errors:
        print(f"\nErrors reading {len(errors)} files:")
        for file_path, error in errors:
            print(f"  {file_path}: {error}")
        print()
    
    # Report schema variations
    print(f"\nFound {len(schema_variations)} distinct schema variations:")
    for i, (schema_sig, file_list) in enumerate(schema_variations.items(), 1):
        print(f"\nVariation {i} ({len(file_list)} files):")
        if args.field:
            for field_name, encoding in schema_sig:
                if field_name == args.field:
                    print(f"  {field_name}: {encoding}")
        else:
            for field_name, encoding in schema_sig:
                print(f"  {field_name}: {encoding}")
        
        if len(file_list) <= 5:
            print(f"  Files: {file_list}")
        else:
            print(f"  First few files: {file_list[:3]}")
            print(f"  Last few files: {file_list[-2:]}")
    
    # Field encoding summary
    if not args.field:
        print(f"\nField encoding summary:")
        for field_name in sorted(field_encodings.keys()):
            encodings = field_encodings[field_name]
            if len(encodings) > 1:
                print(f"  {field_name}: INCONSISTENT")
                for encoding, count in encodings.most_common():
                    print(f"    {encoding}: {count} files")
            else:
                encoding = next(iter(encodings))
                print(f"  {field_name}: {encoding} (all {sum(encodings.values())} files)")
    
    # Focus on problematic fields
    problematic_fields = [field for field, encodings in field_encodings.items() if len(encodings) > 1]
    if problematic_fields:
        print(f"\nProblematic fields with inconsistent encoding:")
        for field in problematic_fields:
            print(f"  {field}")
            encodings = field_encodings[field]
            for encoding, count in encodings.most_common():
                percentage = 100 * count / sum(encodings.values())
                print(f"    {encoding}: {count} files ({percentage:.1f}%)")


if __name__ == "__main__":
    main()