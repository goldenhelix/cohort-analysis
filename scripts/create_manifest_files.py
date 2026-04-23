#!/usr/bin/env python3
"""
Create manifest files from a directory of files, filtering for files that contain
samples not yet seen in a provided samples list. Before writing manifests, each
qualifying VCF is scanned for required FORMAT field declarations (DP, GQ, as
required by the run's min_dp / min_gq) and classified as gVCF (declares END in
INFO) or joint-called (no END). Mixed cohorts are rejected so downstream filter
expressions compose unambiguously.
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from cohort_utils import load_config_file, parse_int


def get_sample_names_from_file(file_path: str) -> List[str]:
    result = subprocess.run(
        ["bcftools", "query", "-l", file_path],
        capture_output=True,
        text=True,
        check=True
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def get_vcf_header(file_path: str) -> str:
    result = subprocess.run(
        ["bcftools", "view", "-h", file_path],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


_HEADER_FIELD_RE = re.compile(r'^##(INFO|FORMAT)=<ID=([^,>]+)', re.MULTILINE)


def declared_fields(header: str) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {"INFO": set(), "FORMAT": set()}
    for kind, field_id in _HEADER_FIELD_RE.findall(header):
        out[kind].add(field_id)
    return out


def read_existing_samples(samples_file: str) -> Set[str]:
    existing_samples: Set[str] = set()
    with open(samples_file, 'r') as f:
        for line in f:
            sample = line.strip()
            if sample:
                existing_samples.add(sample)
    return existing_samples


def find_files_with_new_samples(directory: str, existing_samples: Set[str]) -> List[str]:
    files_with_new_samples: List[str] = []

    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise ValueError(f"{directory} is not a valid directory")

    for file_path in sorted(dir_path.rglob('*.vcf.gz')):
        if not file_path.is_file():
            continue
        try:
            sample_names = get_sample_names_from_file(str(file_path))
            print(f"Processing {file_path}: found {sample_names}")
            if not sample_names:
                continue
            if any(sample in existing_samples for sample in sample_names):
                print(f"  Skipping {file_path}: sample already exists in cohort")
                continue
            files_with_new_samples.append(str(file_path))
        except Exception as e:
            print(f"Warning: Failed to process {file_path}: {e}")

    return files_with_new_samples


def validate_and_classify(
    files: List[str],
    min_dp: int,
    min_gq: int,
) -> Tuple[Dict[str, str], List[str]]:
    """Scan each VCF header. Returns (classification, errors).

    classification maps file_path -> "gvcf" (END declared) or "joint" (not).
    errors is a list of human-readable validation failures aggregated across
    every file. Callers should fail if errors is non-empty.
    """
    classification: Dict[str, str] = {}
    errors: List[str] = []

    for file_path in files:
        try:
            header = get_vcf_header(file_path)
        except subprocess.CalledProcessError as e:
            errors.append(f"  {file_path}: could not read VCF header ({e})")
            continue

        fields = declared_fields(header)

        missing = []
        if min_dp > 0 and "DP" not in fields["FORMAT"]:
            missing.append("DP (required because min_dp > 0)")
        if min_gq > 0 and "GQ" not in fields["FORMAT"]:
            missing.append("GQ (required because min_gq > 0)")
        if missing:
            errors.append(f"  {file_path}: missing FORMAT declaration for {', '.join(missing)}")

        classification[file_path] = "gvcf" if "END" in fields["INFO"] else "joint"

    return classification, errors


def check_cohort_homogeneous(classification: Dict[str, str]) -> Optional[str]:
    kinds = set(classification.values())
    if len(kinds) <= 1:
        return None
    gvcfs = [p for p, k in classification.items() if k == "gvcf"]
    joints = [p for p, k in classification.items() if k == "joint"]
    lines = [
        "Mixed gVCF / joint-VCF cohort detected. This workflow requires all",
        "inputs to be the same kind, because the filter expression for gVCFs",
        "must guard against null QUAL on reference-confident blocks (END > 0)",
        "while joint VCFs do not declare END. Split into separate cohorts, or",
        "set min_qual=0 to disable the QUAL filter entirely.",
        "",
        f"  gVCF (declare INFO/END): {len(gvcfs)} file(s)",
    ]
    lines.extend(f"    {p}" for p in gvcfs[:5])
    if len(gvcfs) > 5:
        lines.append(f"    ... and {len(gvcfs) - 5} more")
    lines.append(f"  joint-called (no INFO/END): {len(joints)} file(s)")
    lines.extend(f"    {p}" for p in joints[:5])
    if len(joints) > 5:
        lines.append(f"    ... and {len(joints) - 5} more")
    return "\n".join(lines)


def write_manifest_files(
    files: List[str],
    output_prefix: str,
    files_per_manifest: int,
    cohort_type: str,
):
    if not files:
        print("No files to write to manifests")
        return

    with open('manifest_list.csv', 'w') as p:
        p.write("manifest_file,cohort_type\n")

        num_manifests = (len(files) + files_per_manifest - 1) // files_per_manifest
        for i in range(num_manifests):
            start_idx = i * files_per_manifest
            end_idx = min(start_idx + files_per_manifest, len(files))
            manifest_files = sorted(files[start_idx:end_idx])

            manifest_filename = f"{output_prefix}_{i + 1:03d}.manifest.txt"
            with open(manifest_filename, 'w') as f:
                for file_path in manifest_files:
                    f.write(f"{file_path}\n")

            print(f"Created {manifest_filename} with {len(manifest_files)} files")
            p.write(f"{manifest_filename},{cohort_type}\n")

        print(f"\nTotal: {len(files)} files across {num_manifests} manifest(s), cohort_type={cohort_type}")


def main():
    parser = argparse.ArgumentParser(
        description="Create manifest files for VCFs containing new samples, "
                    "with header-scan validation of required FORMAT fields "
                    "and classification of the cohort as gVCF or joint.",
    )
    parser.add_argument(
        "samples_file",
        help="Text file containing existing sample names (one per line)",
    )
    parser.add_argument(
        "directory",
        help="Directory containing files to search",
    )
    parser.add_argument(
        "-o", "--output-prefix",
        default="manifest",
        help="Output prefix for manifest files (default: manifest)",
    )
    parser.add_argument(
        "-n", "--files-per-manifest",
        type=int,
        default=128,
        help="Number of files per manifest (default: 128)",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Cohort parameter file (key=value per line). Provides min_dp, min_gq, min_qual.",
    )

    args = parser.parse_args()

    config = load_config_file(args.config)
    min_qual = parse_int(config.get("min_qual"), 10)
    min_dp = parse_int(config.get("min_dp"), 3)
    min_gq = parse_int(config.get("min_gq"), 20)
    print(f"Thresholds: min_qual={min_qual}, min_dp={min_dp}, min_gq={min_gq}")

    print(f"Reading existing samples from {args.samples_file}...")
    existing_samples = read_existing_samples(args.samples_file)
    print(f"Found {len(existing_samples)} existing samples")

    print(f"\nSearching for files with new samples in {args.directory}...")
    files_with_new_samples = find_files_with_new_samples(args.directory, existing_samples)
    print(f"Found {len(files_with_new_samples)} files with new samples")
    if not files_with_new_samples:
        sys.exit(1)

    print("\nValidating VCF headers...")
    classification, errors = validate_and_classify(files_with_new_samples, min_dp, min_gq)

    if errors:
        print("\nError: header validation failed for the following file(s):", file=sys.stderr)
        for err in errors:
            print(err, file=sys.stderr)
        print(
            "\nEvery input VCF must declare the FORMAT fields referenced by the "
            "configured thresholds. Either ensure the inputs declare these fields "
            "or set the corresponding threshold to 0 in the cohort parameter file.",
            file=sys.stderr,
        )
        sys.exit(1)

    mix_error = check_cohort_homogeneous(classification)
    if mix_error:
        print(f"\nError: {mix_error}", file=sys.stderr)
        sys.exit(1)

    cohort_type = next(iter(set(classification.values())))
    print(f"\nCohort type: {cohort_type} ({len(files_with_new_samples)} file(s))")

    print("\nCreating manifest files...")
    write_manifest_files(
        files_with_new_samples,
        args.output_prefix,
        args.files_per_manifest,
        cohort_type,
    )


if __name__ == "__main__":
    main()
