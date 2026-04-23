#!/usr/bin/env python3
"""
Shared utilities for the Cohort Allele Frequency workflow scripts.
"""

import math
import os
import re
import select
import subprocess
import sys


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


def parse_bool(value, default=False):
    """Parse a CLI string into a bool. Argparse passes values through as strings
    when they come from workflow-shell interpolation, so we normalize here."""
    if value is None or value == "":
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


_SLUG_SPACE_RE = re.compile(r"\s+")
_SLUG_STRIP_RE = re.compile(r"[^a-z0-9_\-]")
_SLUG_COLLAPSE_RE = re.compile(r"_+")


def slugify(name):
    """Produce a filesystem- and metadata-safe identifier from a human name.
    Used to derive series_name from cohort_name when the user leaves
    series_name blank on the workflow form."""
    s = str(name).strip().lower()
    s = _SLUG_SPACE_RE.sub("_", s)
    s = _SLUG_STRIP_RE.sub("", s)
    s = _SLUG_COLLAPSE_RE.sub("_", s)
    return s.strip("_-")


_SERIES_RE = re.compile(r'"seriesName"\s*:\s*"([^"]+)"')
_VF_SOURCE_RE = re.compile(
    r'"name"\s*:\s*"([^"]+? Variant Frequencies(?:\s*\(\d+\s*Samples?\))?)"'
)
_COHORT_NAME_RE = re.compile(
    r"^(.+?) Variant Frequencies(?:\s*\(\d+\s*Samples?\))?$"
)


def scrape_cohort_identity(tsf_path, gautil_path):
    """Recover (cohort_name, series_name) from a cohort allele-frequency TSF.

    series_name is stored directly in the TSF's sourceMeta. cohort_name is
    reconstructed from the cohort source field's name, which has the form
    "{cohort_name} Variant Frequencies (N Samples)" (the " (N Samples)" suffix
    is added by gautil's additiveCountAlleles step at merge time).

    The schema JSON can contain an embedded HTML blob with unescaped characters
    that break strict JSON parsing, so we grep the raw text instead of parsing.
    """
    raw = subprocess.run(
        [gautil_path, "schema", tsf_path],
        capture_output=True, text=True, check=True,
    ).stdout
    s_match = _SERIES_RE.search(raw)
    if not s_match:
        sys.exit(
            f"Error: TSF {tsf_path!r} does not declare a seriesName. "
            "Does not appear to be a cohort allele-frequency track."
        )
    series_name = s_match.group(1)

    n_match = _VF_SOURCE_RE.search(raw)
    if not n_match:
        sys.exit(
            f"Error: TSF {tsf_path!r} has no 'X Variant Frequencies' source. "
            "Does not appear to be a cohort allele-frequency track."
        )
    c_match = _COHORT_NAME_RE.match(n_match.group(1))
    if not c_match:
        sys.exit(
            f"Error: unexpected source-name format in {tsf_path!r}: "
            f"{n_match.group(1)!r}. Cannot recover cohort_name."
        )
    return c_match.group(1), series_name


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
