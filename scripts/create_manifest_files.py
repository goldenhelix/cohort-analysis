#!/usr/bin/env python3
"""
Create manifest files from a directory of files, filtering for files that contain
samples not yet seen in a provided samples list.
"""

import argparse
import os
import subprocess
from pathlib import Path
from typing import List, Set


def get_sample_names_from_file(file_path: str) -> List[str]:
    result = subprocess.run(
        ["bcftools", "query", "-l", file_path],
        capture_output=True,
        text=True,
        check=True
    )
    sample_names = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return sample_names


def read_existing_samples(samples_file: str) -> Set[str]:
    existing_samples = set()
    with open(samples_file, 'r') as f:
        for line in f:
            sample = line.strip()
            if sample:  # Skip empty lines
                existing_samples.add(sample)
    return existing_samples


def find_files_with_new_samples(directory: str, existing_samples: Set[str]) -> List[str]:
    files_with_new_samples = []

    # Get all files in the directory (non-recursive)
    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise ValueError(f"{directory} is not a valid directory")

    for file_path in sorted(dir_path.iterdir()):
        if file_path.is_file():
            try:
                # if the extension is not 'vcf.gz' continue
                if file_path.suffix != '.gz' or file_path.stem.split('.')[-1] != 'vcf':
                    continue
         
                # Get sample names from this file
                sample_names = get_sample_names_from_file(str(file_path))
                print(f"Processing {file_path}: found {sample_names}")
                if len(sample_names) == 0:
                    continue

                sample_exists = any(sample in existing_samples for sample in sample_names)
                if sample_exists:
                    print(f"  Skipping {file_path}: sample already exists in cohort")
                    continue

                files_with_new_samples.append(str(file_path))

            except Exception as e:
                print(f"Warning: Failed to process {file_path}: {e}")
                continue

    return files_with_new_samples


def write_manifest_files(files: List[str], output_prefix: str, files_per_manifest: int = 128):
    if not files:
        print("No files to write to manifests")
        return


    with open('manifest_list.csv', 'w') as p:
        p.write(f"manifest_file\n")

        num_manifests = (len(files) + files_per_manifest - 1) // files_per_manifest
        for i in range(num_manifests):
            start_idx = i * files_per_manifest
            end_idx = min(start_idx + files_per_manifest, len(files))
            manifest_files = files[start_idx:end_idx]

            # sort the manifest files for consistency
            manifest_files.sort()

            # Create manifest filename with zero-padded number
            output_suffix = f"{i+1:03d}"
            manifest_filename = f"{output_prefix}_{output_suffix}.manifest.txt"

            with open(manifest_filename, 'w') as f:
                for file_path in manifest_files:
                    f.write(f"{file_path}\n")

            print(f"Created {manifest_filename} with {len(manifest_files)} files")
            p.write(f"{manifest_filename}\n")

        print(f"\nTotal: {len(files)} files across {num_manifests} manifest(s)")


def main():
    parser = argparse.ArgumentParser(
        description="Create manifest files for files containing new samples"
    )
    parser.add_argument(
        "samples_file",
        help="Text file containing existing sample names (one per line)"
    )
    parser.add_argument(
        "directory",
        help="Directory containing files to search"
    )
    parser.add_argument(
        "-o", "--output-prefix",
        default="manifest",
        help="Output prefix for manifest files (default: manifest)"
    )
    parser.add_argument(
        "-n", "--files-per-manifest",
        type=int,
        default=128,
        help="Number of files per manifest (default: 128)"
    )

    args = parser.parse_args()

    # Read existing samples
    print(f"Reading existing samples from {args.samples_file}...")
    existing_samples = read_existing_samples(args.samples_file)
    print(f"Found {len(existing_samples)} existing samples")

    # Find files with new samples
    print(f"\nSearching for files with new samples in {args.directory}...")
    files_with_new_samples = find_files_with_new_samples(args.directory, existing_samples)
    print(f"Found {len(files_with_new_samples)} files with new samples")

    # Write manifest files
    if files_with_new_samples:
        print(f"\nCreating manifest files...")
        write_manifest_files(
            files_with_new_samples,
            args.output_prefix,
            args.files_per_manifest
        )
    else:
        print("\nNo files with new samples found. No manifests created.")


if __name__ == "__main__":
    main()
