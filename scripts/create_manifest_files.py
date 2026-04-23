#!/usr/bin/env python3
"""
Manifest stage of the Build Cohort Annotation Track workflow.

Given a user's typed cohort parameters plus an optional existing-counts TSF,
this script:

  1. Resolves the effective cohort identity (cohort_name, series_name,
     existing_counts). If the user picked a TSF — or if one is auto-discovered
     by globbing for a matching series_name — its embedded metadata is the
     source of truth and overrides typed values (loudly, with a log line).
  2. Reads the sample list already in the cohort (if existing_counts resolved),
     so we can skip input VCFs that would re-add an existing sample.
  3. Scans the input directory for `*.vcf.gz` files with new samples.
  4. Validates each candidate VCF's header against the active thresholds:
     DP must be declared when min_dp > 0; GQ when min_gq > 0. Errors are
     aggregated across all files and reported once.
  5. Classifies the cohort as gVCF (every input declares ##INFO=<ID=END,...>)
     or joint (none do). Mixed cohorts are rejected with the full per-category
     file list.
  6. Writes one or more manifest files plus a manifest_list.csv with the
     cohort_type column for downstream stages.
  7. Writes resolved identity + guardrail outputs to the supplied
     --emit-env file so the workflow task can fold them into its result.env.
"""

import argparse
import glob
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from cohort_utils import parse_bool, scrape_cohort_identity, slugify


COHORTS_DIR_REL = "AppData/Common Data/UserAnnotations/cohorts"


def get_sample_names_from_file(file_path: str) -> List[str]:
    result = subprocess.run(
        ["bcftools", "query", "-l", file_path],
        capture_output=True, text=True, check=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def get_vcf_header(file_path: str) -> str:
    return subprocess.run(
        ["bcftools", "view", "-h", file_path],
        capture_output=True, text=True, check=True,
    ).stdout


_HEADER_FIELD_RE = re.compile(r"^##(INFO|FORMAT)=<ID=([^,>]+)", re.MULTILINE)


def declared_fields(header: str) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {"INFO": set(), "FORMAT": set()}
    for kind, field_id in _HEADER_FIELD_RE.findall(header):
        out[kind].add(field_id)
    return out


def resolve_existing_counts(
    existing_counts: Optional[str],
    cohort_name: Optional[str],
    series_name: Optional[str],
    workspace_dir: str,
) -> str:
    """Return the path to the TSF we should extend, or '' for a new cohort.

    Precedence: user-provided file > glob for latest `{series}_*.tsf`.
    The glob only runs when we can determine a series_name (either typed or
    slugified from cohort_name)."""
    if existing_counts:
        if not os.path.exists(existing_counts):
            sys.exit(f"Error: existing_counts path {existing_counts!r} does not exist.")
        return existing_counts

    hypothetical_series = series_name or (slugify(cohort_name) if cohort_name else None)
    if not hypothetical_series:
        return ""

    pattern = os.path.join(workspace_dir, COHORTS_DIR_REL, f"{hypothetical_series}_*.tsf")
    matches = sorted(glob.glob(pattern))
    if matches:
        return matches[-1]
    return ""


def resolve_identity(
    typed_cohort: Optional[str],
    typed_series: Optional[str],
    existing_counts: str,
    gautil_path: str,
) -> Tuple[str, str]:
    """Decide the effective (cohort_name, series_name) for this run.

    If existing_counts is set, its metadata overrides typed values (loudly).
    Otherwise we require cohort_name and derive series_name from it if blank.
    """
    if existing_counts:
        tsf_cohort, tsf_series = scrape_cohort_identity(existing_counts, gautil_path)
        if typed_cohort and typed_cohort != tsf_cohort:
            print(
                f"Note: using cohort_name from existing TSF: {tsf_cohort!r} "
                f"(ignoring typed value {typed_cohort!r})"
            )
        if typed_series and typed_series != tsf_series:
            print(
                f"Note: using series_name from existing TSF: {tsf_series!r} "
                f"(ignoring typed value {typed_series!r})"
            )
        return tsf_cohort, tsf_series

    if not typed_cohort:
        sys.exit(
            "Error: cohort_name is required when starting a new cohort "
            "(no existing cohort TSF was selected or auto-discovered)."
        )
    cohort_name = typed_cohort
    series_name = typed_series or slugify(cohort_name)
    if not typed_series:
        print(
            f"Note: deriving series_name={series_name!r} from "
            f"cohort_name={cohort_name!r}"
        )
    return cohort_name, series_name


def read_existing_samples(existing_counts: str, gautil_path: str) -> Set[str]:
    """Use gautil to enumerate the sample names already in the cohort TSF."""
    if not existing_counts:
        return set()
    txsource = (
        "{\n"
        '  "inputs": ["' + existing_counts + ':2"],\n'
        '  "name": "entitiesAsFeatures",\n'
        '  "options": { "className": "EntitiesAsFeaturesTransformOptions" }\n'
        "}\n"
    )
    path = "sample_names.txsource"
    with open(path, "w") as f:
        f.write(txsource)
    out = subprocess.run(
        [gautil_path, "read", path],
        capture_output=True, text=True, check=True,
    ).stdout
    samples: Set[str] = set()
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) >= 5 and parts[4].strip():
            samples.add(parts[4].strip())
    print(f"Existing cohort contains {len(samples)} sample(s).")
    return samples


def find_files_with_new_samples(directory: str, existing_samples: Set[str]) -> List[str]:
    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise ValueError(f"{directory} is not a valid directory")

    found: List[str] = []
    for file_path in sorted(dir_path.rglob("*.vcf.gz")):
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
            found.append(str(file_path))
        except Exception as e:
            print(f"Warning: Failed to process {file_path}: {e}")
    return found


def validate_and_classify(
    files: List[str], min_dp: int, min_gq: int
) -> Tuple[Dict[str, str], List[str]]:
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
) -> str:
    """Write per-batch manifest files plus manifest_list.csv. Returns the
    absolute path to manifest_list.csv."""
    manifest_list_path = os.path.abspath("manifest_list.csv")
    with open(manifest_list_path, "w") as p:
        p.write("manifest_file,cohort_type\n")

        num_manifests = (len(files) + files_per_manifest - 1) // files_per_manifest
        for i in range(num_manifests):
            start = i * files_per_manifest
            end = min(start + files_per_manifest, len(files))
            batch = sorted(files[start:end])
            manifest_filename = f"{output_prefix}_{i + 1:03d}.manifest.txt"
            with open(manifest_filename, "w") as f:
                for file_path in batch:
                    f.write(f"{file_path}\n")
            print(f"Created {manifest_filename} with {len(batch)} files")
            p.write(f"{manifest_filename},{cohort_type}\n")

        print(
            f"\nTotal: {len(files)} files across {num_manifests} manifest(s), "
            f"cohort_type={cohort_type}"
        )
    return manifest_list_path


def emit_env(path: str, values: Dict[str, str]) -> None:
    """Write key=value lines to the given path, so the task's bash step can
    fold them into result.env for downstream stages."""
    with open(path, "w") as f:
        for key, val in values.items():
            f.write(f"{key}={val}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Resolve cohort identity, validate input VCFs, and write "
                    "per-batch manifests for the Build Cohort Annotation Track workflow.",
    )
    parser.add_argument("--input-directory", required=True)
    parser.add_argument("--cohort-name", default="")
    parser.add_argument("--series-name", default="")
    parser.add_argument("--existing-counts", default="")
    parser.add_argument("--min-qual", type=int, default=10)
    parser.add_argument("--min-dp", type=int, default=3)
    parser.add_argument("--min-gq", type=int, default=10)
    parser.add_argument("--include-reference-confident-loci", default="true")
    parser.add_argument("--files-per-manifest", type=int, default=128)
    parser.add_argument("--output-prefix", default="manifest")
    parser.add_argument(
        "--emit-env", required=True,
        help="Path to write resolved key=value pairs for the task's result.env",
    )

    args = parser.parse_args()

    workspace_dir = os.environ.get("WORKSPACE_DIR", "")
    gautil_path = os.environ.get("GAUTIL_PATH", "/opt/apiserver/gautil")
    include_rcl = parse_bool(args.include_reference_confident_loci, True)

    print(
        f"Thresholds: min_qual={args.min_qual}, min_dp={args.min_dp}, "
        f"min_gq={args.min_gq}, include_reference_confident_loci={include_rcl}"
    )

    existing_counts = resolve_existing_counts(
        args.existing_counts or None,
        args.cohort_name or None,
        args.series_name or None,
        workspace_dir,
    )
    if existing_counts:
        print(f"Existing cohort TSF: {existing_counts}")
    else:
        print("No existing cohort TSF; starting a new cohort.")

    cohort_name, series_name = resolve_identity(
        args.cohort_name or None,
        args.series_name or None,
        existing_counts,
        gautil_path,
    )
    print(f"Resolved cohort_name={cohort_name!r}, series_name={series_name!r}")

    existing_samples = read_existing_samples(existing_counts, gautil_path)

    print(f"\nSearching for VCFs with new samples in {args.input_directory}...")
    files_with_new_samples = find_files_with_new_samples(args.input_directory, existing_samples)
    print(f"Found {len(files_with_new_samples)} file(s) with new samples.")
    if not files_with_new_samples:
        sys.exit(1)

    print("\nValidating VCF headers...")
    classification, errors = validate_and_classify(
        files_with_new_samples, args.min_dp, args.min_gq
    )
    if errors:
        print("\nError: header validation failed for the following file(s):", file=sys.stderr)
        for err in errors:
            print(err, file=sys.stderr)
        print(
            "\nEvery input VCF must declare the FORMAT fields referenced by the "
            "configured thresholds. Either ensure the inputs declare these fields "
            "or set the corresponding threshold to 0 on the workflow.",
            file=sys.stderr,
        )
        sys.exit(1)

    mix_error = check_cohort_homogeneous(classification)
    if mix_error:
        print(f"\nError: {mix_error}", file=sys.stderr)
        sys.exit(1)

    cohort_type = next(iter(set(classification.values())))
    print(f"\nCohort type: {cohort_type} ({len(files_with_new_samples)} file(s))")

    manifest_list_path = write_manifest_files(
        files_with_new_samples,
        args.output_prefix,
        args.files_per_manifest,
        cohort_type,
    )

    emit_env(
        args.emit_env,
        {
            "cohort_name": cohort_name,
            "series_name": series_name,
            "existing_counts": existing_counts,
            "cohort_type": cohort_type,
            "resolved_manifest_list": manifest_list_path,
        },
    )


if __name__ == "__main__":
    main()
