#!/usr/bin/env python3
"""
Utility functions for running subprocess commands with real-time output streaming.
"""

import math
import os
import subprocess
import sys
import select

def load_config_file(config_path):
    """Load configuration from a key=value parameter file."""
    config = {}
    with open(config_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue

            # Parse key=value
            if '=' not in line:
                print(f"Warning: Skipping invalid line {line_num}: {line}", file=sys.stderr)
                continue

            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip()

            # Remove quotes if present
            if (value.startswith('"') and value.endswith('"')) or \
               (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]

            config[key] = value

    # Required parameters
    required_params = ['cohort_name', 'series_name']
    for param in required_params:
        if param not in config:
            print(f"Error: Required parameter '{param}' not found in config file", file=sys.stderr)
            sys.exit(1)

    return config


def calculate_thread_counts(cpu_count, file_count):
    # the readers per flattener is also the number of stripes

    # Need to have a better way to control this so we can specify the stripes seperate from the merge threads
    #thread_count = cpu_count * 4 
    #flatten_threads = cpu_count * 4
    #readers_per_flattener = math.ceil(file_count / (flatten_threads -1))
    readers_per_flattener = 1

    # This is the ratio of reader threads to flatten threads
    #remaining_threads = thread_count - flatten_threads
    #reader_threads = max(1, math.floor(remaining_threads / flatten_threads))
    reader_threads = 5 #max(1, math.floor(remaining_threads / flatten_threads))

    merge_threads = ((file_count + readers_per_flattener) / readers_per_flattener)
    total = (merge_threads * reader_threads) + merge_threads

    print(f"cpu_count: {cpu_count}")
    #print(f"flatten_threads: {flatten_threads}")
    print(f"readers_per_flattener: {readers_per_flattener}")
    print(f"reader_threads: {reader_threads}")
    print(f"total_threads: {total}")

    return reader_threads, readers_per_flattener


def get_env_or_error(var_name):
    """Get environment variable or exit with error."""
    value = os.environ.get(var_name)
    if value is None:
        print(f"Error: Environment variable {var_name} is not set", file=sys.stderr)
        sys.exit(1)
    return value


def quote_if_needed(value):
    """Add quotes if value doesn't already contain them."""
    if '"' in value:
        return value
    return f'"{value}"'



def run_process_with_filtered_output(command, filter_warnings=None):
    """
    Run a subprocess and stream output in real-time with optional filtering.

    Args:
        command: List of command arguments to execute
        filter_warnings: Optional list of strings to filter from stderr

    Returns:
        int: Return code from the process

    Raises:
        SystemExit: If process fails with non-zero return code
    """
    if filter_warnings is None:
        filter_warnings = []

    process = subprocess.Popen(
        command,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1  # Line buffered
    )

    # Stream output in real-time from both stdout and stderr
    streams = {
        process.stdout: sys.stdout,
        process.stderr: sys.stderr
    }

    while process.poll() is None or any(streams):
        # Wait for any stream to be readable
        readable, _, _ = select.select(list(streams.keys()), [], [], 0.1)

        for stream in readable:
            line = stream.readline()
            if line:
                if stream == process.stderr:
                    # Filter out specified warning messages
                    should_print = True
                    for filter_str in filter_warnings:
                        if filter_str in line:
                            should_print = False
                            break
                    if should_print:
                        print(line, end='', file=streams[stream])
                else:
                    print(line, end='', file=streams[stream])
            else:
                # Stream closed
                streams.pop(stream)

    return_code = process.wait()
    if return_code != 0:
        print(f"Error: Command failed with return code {return_code}", file=sys.stderr)
        sys.exit(return_code)

    return return_code
