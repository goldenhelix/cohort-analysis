#!/usr/bin/env python3
"""
Cohort Annotation Track Updater
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime
from pathlib import Path
import json

from cohort_utils import (
    calculate_thread_counts,
    get_env_or_error,
    load_config_file,
    parse_bool,
    parse_int,
    run_process_with_filtered_output,
)


def check_tbi_files(manifest_file):
    """Verify that TBI index files exist for all VCF files in the manifest."""
    print("Checking for TBI index files...")
    missing_tbi_files = []

    with open(manifest_file, 'r') as f:
        for line in f:
            vcf_file = line.strip()
            if not vcf_file:
                continue

            if vcf_file.endswith('.vcf.gz'):
                tbi_file = f"{vcf_file}.tbi"
                if not os.path.exists(tbi_file):
                    missing_tbi_files.append(tbi_file)

    if missing_tbi_files:
        print("Error: The following TBI index files are missing:", file=sys.stderr)
        for missing_file in missing_tbi_files:
            print(f"  {missing_file}", file=sys.stderr)
        sys.exit(1)

    print("All TBI index files found.")


def classify_manifest_cohort(manifest_file):
    """Peek at the first VCF in the manifest and return 'gvcf' if its header
    declares ##INFO=<ID=END,...>, else 'joint'. Upstream (create_manifest_files)
    has already validated that every file in the manifest shares a single
    classification, so a single probe suffices."""
    first_vcf = None
    with open(manifest_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                first_vcf = line
                break
    if not first_vcf:
        print(f"Error: Manifest {manifest_file} contains no VCF entries.", file=sys.stderr)
        sys.exit(1)
    header = subprocess.run(
        ["bcftools", "view", "-h", first_vcf],
        capture_output=True, text=True, check=True,
    ).stdout
    for line in header.splitlines():
        if line.startswith("##INFO=<ID=END,"):
            return "gvcf"
    return "joint"


def build_filter_section(min_qual, min_dp, min_gq, cohort_type, include_reference_confident_loci):
    """Compose the filterByExpr block. INFO-level `expr` guards null QUAL on
    gVCF reference-confident blocks by short-circuiting on END when the
    cohort is gVCF and we want those blocks preserved; joint cohorts use a
    plain QUAL threshold because they have no reference-confident records.
    FORMAT-level `sampleExpr` joins the active per-sample thresholds with
    `and`."""
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
    parser = argparse.ArgumentParser(
        description='Update cohort variant frequencies track by adding new samples',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--config',
        required=True,
        help='Configuration file with parameters (key=value per line)'
    )
    parser.add_argument(
        '--manifest-file',
        required=True,
        help='Path to the manifest file containing VCF file paths'
    )

    args = parser.parse_args()

    # Load configuration from file
    config = load_config_file(args.config)
    manifest_file = args.manifest_file

    # Parse parameters with defaults
    out_file = manifest_file.replace('.manifest.txt', '.tsf')
    out_file_base = out_file[:-4]  # Remove .tsf extension

    if(os.path.exists(out_file)):
      os.remove(out_file)

    cohort_name = config['cohort_name']
    series_name = config['series_name']
    sample_name_threshold = int(config.get('sample_name_threshold', '20'))
    min_qual = parse_int(config.get('min_qual'), 10)
    min_dp = parse_int(config.get('min_dp'), 3)
    min_gq = parse_int(config.get('min_gq'), 20)
    include_reference_confident_loci = parse_bool(
        config.get('include_reference_confident_loci'), True
    )

    gautil_path = os.environ.get('GAUTIL_PATH', '/opt/apiserver/gautil')

    # Get required environment variables
    workspace_dir = get_env_or_error('WORKSPACE_DIR')
    gh_workspace_assembly = get_env_or_error('GH_WORKSPACE_ASSEMBLY')

    # Optional environment variables
    agent_cpu_cores = int(os.environ.get('AGENT_CPU_CORES'))
    agent_memory_gb = int(os.environ.get('AGENT_MEMORY_GB'))
    task_dir = os.environ.get('TASK_DIR')

    # Track parameters
    source_name = f"{cohort_name} Variant Frequencies"
    source_version = datetime.utcnow().strftime("%Y-%m-%d")

    # Determine coordinate system
    if gh_workspace_assembly.startswith('GRCh_37'):
        coord_sys_id = "GRCh_37_g1k,Chromosome,Homo sapiens"
    else:
        coord_sys_id = "GRCh_38,Chromosome,Homo sapiens"

    print(f"Workspace Assembly: {gh_workspace_assembly} => {coord_sys_id}")

    # Check for TBI index files
    check_tbi_files(manifest_file)


    # Annotations folder
    annotations_folder = os.path.join(workspace_dir, "AppData/Common Data/Annotations")
    if not os.path.exists(annotations_folder):
      print(f"Could not find annotations directory: {annotationFolder}")
      sys.exit(1)

    # Set up environment for gautil
    os.environ['GOLDENHELIX_USERDATA'] = os.path.join(workspace_dir, 'AppData')
    crash_dump_dir = os.path.join(workspace_dir, 'AppData/VarSeq/User Data')
    if os.path.exists(crash_dump_dir):
        os.environ['GH_CRASH_DUMP_DIR'] = crash_dump_dir

    cohort_type = classify_manifest_cohort(manifest_file)
    print(f"Cohort type for this manifest: {cohort_type}")
    print(
        f"Thresholds: min_qual={min_qual}, min_dp={min_dp}, min_gq={min_gq}, "
        f"include_reference_confident_loci={include_reference_confident_loci}"
    )

    filter_by_expr_section = build_filter_section(
        min_qual, min_dp, min_gq, cohort_type, include_reference_confident_loci
    )
    if filter_by_expr_section:
        print("Filter section:\n" + filter_by_expr_section)
    else:
        print("No filter thresholds set; skipping filterByExpr.")

    print(f"CPU cores: {agent_cpu_cores}")
    print(f"Memory (GB): {agent_memory_gb}")

    file_count = 0
    with open(manifest_file, 'r') as f:
        for line in f:
            vcf_file = line.strip()
            if not vcf_file:
              continue

            file_count += 1

    if file_count == 0:
      print(f"No VCF files found in manifest file: {manifest_file}")
      sys.exit(1)

    reader_threads, readers_per_flattener = calculate_thread_counts(agent_cpu_cores, file_count)

    print(f"Reader threads: {reader_threads}")
    print(f"Readers per flattener: {readers_per_flattener}")

    # Create gautil batch file
    gautil_batch_content = f"""- forEach:
    inputCount: 1
    taskList:
      - stableSourcePropTransform:
          sourceProps:
            - StringProp:
                name: CombineGVCFSpanRecord
                value: true
      - alleleicPrimitives
{filter_by_expr_section}      - keepFields:
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
      seriesName: "{series_name}"
      sourceVersion: "{source_version}"
"""

    batch_file_path = "gautil_batch_file.yaml"
    with open(batch_file_path, 'w') as f:
        f.write(gautil_batch_content)

    print(f"Created batch file: {batch_file_path}")

    # Run gautil
    print("Running gautil...")
    gautil_cmd = [
        gautil_path, "run",
        f"--annotationFolder={annotations_folder}",
        "--manifest", manifest_file,
        "-c", batch_file_path
    ]

    run_process_with_filtered_output(
        gautil_cmd,
        filter_warnings=["GAFeatureReader loop level greater than 1"]
    )

    # Precompute the output file
    # print("Precomputing output file...")
    # subprocess.run([gautil_path, "precompute", out_file], check=True)

    print(f"Successfully merged VCF files into: {out_file}")


if __name__ == '__main__':
    main()
