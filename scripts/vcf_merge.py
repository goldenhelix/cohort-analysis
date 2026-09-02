#!/usr/bin/env python3
"""
Per-manifest VCF merge stage. Takes one manifest file (a list of VCF paths
with a shared cohort_type), composes a gautil batch that filters and merges
them into an intermediate TSF, and runs it.

The expression composition is cohort-type aware: gVCF cohorts with IRCL
enabled use `END > 0 or QUAL > {min_qual}` so reference-confident blocks
with null QUAL survive; joint cohorts and IRCL-disabled gVCFs use a plain
QUAL threshold. FORMAT-level `sampleExpr` joins active per-sample thresholds
with `and`.
"""

import argparse
import csv
import os
import subprocess
import sys
from datetime import datetime

from cohort_utils import (
    calculate_thread_counts,
    get_env_or_error,
    is_bgzf_vcf,
    parse_bool,
    run_process_with_filtered_output,
)


def check_tbi_files(manifest_file):
    """Verify that TBI index files exist for the bgzip-compressed VCFs in the
    manifest (`.vcf.gz` / `.gvcf.gz`). Uncompressed `.vcf` / `.gvcf` inputs
    have no tabix index to check for."""
    print("Checking for TBI index files...")
    missing = []
    with open(manifest_file, "r") as f:
        for line in f:
            vcf = line.strip()
            if vcf and is_bgzf_vcf(vcf) and not os.path.exists(f"{vcf}.tbi"):
                missing.append(f"{vcf}.tbi")
    if missing:
        print("Error: missing TBI index files:", file=sys.stderr)
        for m in missing:
            print(f"  {m}", file=sys.stderr)
        sys.exit(1)
    print("All TBI index files found.")


def build_filter_section(min_qual, min_dp, min_gq, cohort_type, include_reference_confident_loci):
    """Compose the filterByExpr block. On gVCF cohorts with IRCL=true we
    short-circuit QUAL filtering on END (reference-confident blocks) so their
    null QUAL doesn't drop them; other combinations use plain QUAL.
    FORMAT-level sampleExpr joins active thresholds with `and`."""
    if min_qual > 0:
        if cohort_type == "gvcf" and include_reference_confident_loci:
            info_expr = f"END > 0 or QUAL > {min_qual}"
        else:
            info_expr = f"QUAL > {min_qual}"
    else:
        info_expr = None

    sample_terms = []
    if min_dp > 0:
        sample_terms.append(f"DP > {min_dp}")
    if min_gq > 0:
        sample_terms.append(f"GQ >= {min_gq}")
    sample_expr = " and ".join(sample_terms) if sample_terms else None

    if not info_expr and not sample_expr:
        return ""
    section = "      - filterByExpr:\n"
    if info_expr:
        section += f'          expr: "{info_expr}"\n'
    if sample_expr:
        section += f'          sampleExpr: "{sample_expr}"\n'
    return section


def main():
    parser = argparse.ArgumentParser(description="Per-manifest VCF merge stage.")
    parser.add_argument("--manifest-file", required=True)
    parser.add_argument("--cohort-type", required=True, choices=["gvcf", "joint"])
    parser.add_argument("--series-name", required=True)
    parser.add_argument("--min-qual", type=int, default=10)
    parser.add_argument("--min-dp", type=int, default=3)
    parser.add_argument("--min-gq", type=int, default=10)
    parser.add_argument("--include-reference-confident-loci", default="true")
    args = parser.parse_args()

    manifest_file = args.manifest_file
    include_rcl = parse_bool(args.include_reference_confident_loci, True)

    out_file = manifest_file.replace(".manifest.txt", ".tsf")
    if os.path.exists(out_file):
        os.remove(out_file)

    gautil_path = os.environ.get("GAUTIL_PATH", "/opt/apiserver/gautil")
    workspace_dir = get_env_or_error("WORKSPACE_DIR")
    gh_workspace_assembly = get_env_or_error("GH_WORKSPACE_ASSEMBLY")
    agent_cpu_cores = int(os.environ.get("AGENT_CPU_CORES"))
    agent_memory_gb = int(os.environ.get("AGENT_MEMORY_GB"))

    source_version = datetime.utcnow().strftime("%Y-%m-%d")

    if gh_workspace_assembly.startswith("GRCh_37"):
        coord_sys_id = "GRCh_37_g1k,Chromosome,Homo sapiens"
    else:
        coord_sys_id = "GRCh_38,Chromosome,Homo sapiens"
    print(f"Workspace Assembly: {gh_workspace_assembly} => {coord_sys_id}")

    check_tbi_files(manifest_file)

    annotations_folder = os.path.join(workspace_dir, "AppData/Common Data/Annotations")
    if not os.path.exists(annotations_folder):
        print(f"Could not find annotations directory: {annotations_folder}", file=sys.stderr)
        sys.exit(1)

    os.environ["GOLDENHELIX_USERDATA"] = os.path.join(workspace_dir, "AppData")
    crash_dump_dir = os.path.join(workspace_dir, "AppData/VarSeq/User Data")
    if os.path.exists(crash_dump_dir):
        os.environ["GH_CRASH_DUMP_DIR"] = crash_dump_dir

    print(
        f"Cohort type: {args.cohort_type}; thresholds min_qual={args.min_qual}, "
        f"min_dp={args.min_dp}, min_gq={args.min_gq}, "
        f"include_reference_confident_loci={include_rcl}"
    )
    filter_section = build_filter_section(
        args.min_qual, args.min_dp, args.min_gq, args.cohort_type, include_rcl
    )
    if filter_section:
        print("Filter section:\n" + filter_section)
    else:
        print("No filter thresholds set; skipping filterByExpr.")

    print(f"CPU cores: {agent_cpu_cores}")
    print(f"Memory (GB): {agent_memory_gb}")

    file_count = 0
    with open(manifest_file, "r") as f:
        for line in f:
            if line.strip():
                file_count += 1
    if file_count == 0:
        print(f"No VCF files found in manifest file: {manifest_file}", file=sys.stderr)
        sys.exit(1)

    reader_threads, readers_per_flattener = calculate_thread_counts(agent_cpu_cores, file_count)
    print(f"Reader threads: {reader_threads}")
    print(f"Readers per flattener: {readers_per_flattener}")

    gautil_batch_content = f"""- forEach:
    inputCount: 1
    taskList:
      - stableSourcePropTransform:
          sourceProps:
            - StringProp:
                name: CombineGVCFSpanRecord
                value: true
      - alleleicPrimitives
{filter_section}      - keepFields:
          keepSymbols:
            - RefAlt
            - REF
            - ALT
            - GT
            - END
            - Samples
      - fullyFlattenedMultiAllelicSplit
      - leftAlign
      - trimCommonBases
      - variantCollapsing

- mergeVariantsTransform:
    inputBufferSize: 4000
    onlyMergeMatchingRefAlts: true
    mergeDifferentRecordTypes: false
    readerWorkerThreads: {reader_threads}
    readersPerFlattener: {readers_per_flattener}

- TsfWriterTask:
    filePath: "{out_file}"
    sourceMeta:
      coordSysId: "{coord_sys_id}"
      seriesName: "{args.series_name}"
      sourceVersion: "{source_version}"
"""

    batch_file_path = "gautil_batch_file.yaml"
    with open(batch_file_path, "w") as f:
        f.write(gautil_batch_content)
    print(f"Created batch file: {batch_file_path}")

    print("Running gautil...")
    run_process_with_filtered_output(
        [
            gautil_path, "run",
            f"--annotationFolder={annotations_folder}",
            "--manifest", manifest_file,
            "-c", batch_file_path,
        ],
        filter_warnings=["GAFeatureReader loop level greater than 1"],
    )

    print(f"Successfully merged VCF files into: {out_file}")


if __name__ == "__main__":
    main()
