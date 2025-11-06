#!/usr/bin/env python3
"""
Calculate thread counts for variant merge
"""

import math
import argparse
import os
import sys

def max(a, b):
    return a if a > b else b

def min(a, b):
    return a if a < b else b

def main():
    parser = argparse.ArgumentParser(
        description='Calculate CPUs per file count',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'count',
        type=int,
        help='Number of files'
    )
    parser.add_argument(
        '--reader-threads',
        action='store_true',
        help='Output only the reader_threads count'
    )
    parser.add_argument(
        '--readers-per-flattener',
        action='store_true',
        help='Output only the readers_per_flattener count'
    )
    parser.add_argument(
        '--cpu-count',
        type=int,
        metavar='N',
        help='Number of CPUs (default: auto-detect)'
    )
    
    args = parser.parse_args()
    if args.count <= 0:
        print("Error: File count must be greater than 0", file=sys.stderr)
        sys.exit(1)
    
    # Get the number of CPUs available on the system
    if args.cpu_count is not None:
        if args.cpu_count <= 0:
            print("Error: CPU count must be greater than 0", file=sys.stderr)
            sys.exit(1)
        cpu_count = args.cpu_count
    else:
        cpu_count = os.cpu_count()
        if cpu_count is None:
            print("Error: Could not determine CPU count", file=sys.stderr)
            sys.exit(2)

    # merge count is the number of threads for the merge steps
    file_count = args.count

    flatten_threads = min(cpu_count - 2, file_count)

    remaining_threads = cpu_count - flatten_threads
    # This is the ratio of reader threads to flatten threads
    reader_threads = max(1, math.floor(remaining_threads / flatten_threads))

    readers_per_flattener = math.ceil(file_count / flatten_threads)

    # compute it back out to see the counts after rounding/truncation 
    merge_threads = ((file_count + readers_per_flattener) / readers_per_flattener)
    total = (merge_threads * reader_threads) + merge_threads

    # If specific flags are set, output only that value for easy parsing
    if args.reader_threads:
        print(reader_threads)
    elif args.readers_per_flattener:
        print(readers_per_flattener)
    else:
        # Default: output all values
        print(f"cpu_count: {cpu_count}")
        print(f"flatten_threads: {flatten_threads}")
        print(f"readers_per_flattener: {readers_per_flattener}")
        print(f"reader_threads: {reader_threads}")
        print(f"total_threads: {total}")


if __name__ == '__main__':
    main()
