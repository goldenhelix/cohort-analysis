#!/usr/bin/env python3
"""
Final merge stage. Takes the per-manifest intermediate TSFs produced by
vcf_merge.py and combines them into a single cohort allele-frequency TSF via
gautil's additiveCountAlleles transform.

When include_reference_confident_loci=false we re-introduce the historical
post-merge `any(AlleleCounts > 0)` filter. When true (default) we keep
zero-count loci so gVCF reference-confident blocks continue to carry
evidence-of-absence signal into the cohort catalog.
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime

from cohort_utils import (
    get_env_or_error,
    parse_bool,
    run_process_with_filtered_output,
)


COHORTS_DIR_REL = "AppData/Common Data/UserAnnotations/cohorts"


def main():
    parser = argparse.ArgumentParser(description="Merge per-manifest TSFs into the cohort track.")
    parser.add_argument("--manifest-parameter-file", required=True,
                        help="manifest_list.csv written by the manifest stage")
    parser.add_argument("--cohort-name", required=True)
    parser.add_argument("--series-name", required=True)
    parser.add_argument("--existing-counts", default="",
                        help="Path to existing cohort TSF (resolved by the manifest stage)")
    parser.add_argument("--sample-name-threshold", type=int, default=20)
    parser.add_argument("--include-reference-confident-loci", default="true")
    parser.add_argument("--out-file", default="",
                        help="Optional override for the output TSF path (workspace-relative)")
    args = parser.parse_args()

    include_rcl = parse_bool(args.include_reference_confident_loci, True)
    source_version = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")

    workspace_dir = get_env_or_error("WORKSPACE_DIR")
    gh_workspace_assembly = get_env_or_error("GH_WORKSPACE_ASSEMBLY")
    agent_cpu_cores = int(os.environ.get("AGENT_CPU_CORES"))
    agent_memory_gb = int(os.environ.get("AGENT_MEMORY_GB"))
    gautil_path = os.environ.get("GAUTIL_PATH", "/opt/apiserver/gautil")

    if gh_workspace_assembly.startswith("GRCh_37"):
        coord_sys_id = "GRCh_37_g1k,Chromosome,Homo sapiens"
    else:
        coord_sys_id = "GRCh_38,Chromosome,Homo sapiens"
    print(f"Workspace Assembly: {gh_workspace_assembly} => {coord_sys_id}")

    # Determine output path
    if args.out_file:
        out_file = args.out_file
        if out_file.endswith(".tsf"):
            out_file = out_file[:-4]
    else:
        out_file = os.path.join(COHORTS_DIR_REL, args.series_name)
    out_file = f"{out_file}_{source_version}.tsf"
    out_file = os.path.join(workspace_dir, out_file)

    # Read manifest-list CSV (written by stage 1) to find per-manifest TSFs
    manifest_parameter_file = args.manifest_parameter_file
    print(f"Loading manifest parameter file: {manifest_parameter_file}")
    new_counts_files = []
    with open(manifest_parameter_file, "r") as f:
        header = f.readline()
        print(f"Header: {header.strip()}")
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            cell = line.split(",")[0]
            tsf = cell.replace(".manifest.txt", ".tsf")
            tsf_path = os.path.join(os.path.dirname(manifest_parameter_file), tsf)
            new_counts_files.append(tsf_path)
            print(f"New counts file: {tsf_path}")

    if not new_counts_files:
        print("No new processed files found", file=sys.stderr)
        sys.exit(1)

    existing_counts = args.existing_counts
    existing_counts_samples = ""
    if existing_counts:
        print(f"Using existing counts: {existing_counts}")
        existing_counts_samples = f"{existing_counts}:2"
    else:
        print("No existing counts file; building a new cohort.")

    annotations_folder = os.path.join(workspace_dir, "AppData/Common Data/Annotations")
    if not os.path.exists(annotations_folder):
        print(f"Could not find annotations directory: {annotations_folder}", file=sys.stderr)
        sys.exit(1)

    os.environ["GOLDENHELIX_USERDATA"] = os.path.join(workspace_dir, "AppData")
    crash_dump_dir = os.path.join(workspace_dir, "AppData/VarSeq/User Data")
    if os.path.exists(crash_dump_dir):
        os.environ["GH_CRASH_DUMP_DIR"] = crash_dump_dir

    print(f"CPU cores: {agent_cpu_cores}")
    print(f"Memory (GB): {agent_memory_gb}")

    source_name = f"{args.cohort_name} Variant Frequencies"

    # When IRCL is true (default) we keep loci where no sample has a variant
    # allele, because on gVCF inputs those rows carry the reference-confident
    # evidence that makes cohort allele frequencies meaningful. When IRCL is
    # false we re-introduce the historical any(AlleleCounts > 0) filter.
    ac_filter_section = ""
    if not include_rcl:
        ac_filter_section = (
            "        - filterByExpr:\n"
            "            expr: any(AlleleCounts > 0)\n\n"
        )

    gautil_batch_content = f"""
        - mergeVariantsTransform:
            onlyMergeMatchingRefAlts: true
            mergeDifferentRecordTypes: false
            inputBufferSize: 100
            readerWorkerThreads: 1
            readersPerFlattener: 1

        - additiveCountAlleles:
            existingCountsSource: "{existing_counts}"
            existingCountsSampleSource: "{existing_counts_samples}"
            countNoCalls: true
            sourceNamePrefix: "{source_name}"
            outputSampleNamesThreshold: {args.sample_name_threshold}

{ac_filter_section}        - runTaskLists:
            taskLists:
              - SourceTaskListTask:
                  taskList:
                    - createAnnotation
                    - TsfWriterTask:
                        filePath: "{out_file}"
                        sourceMeta:
                          coordSysId: "{coord_sys_id}"
                          seriesName: "{args.series_name}"
                          sourceVersion: "{source_version}"

              - SourceTaskListTask:
                  taskList:
                    - keepFields:
                        keepSymbols:
                          - Samples
                    - subsetUsageSpace:
                        usageSpace: "[]"

                    - TsfWriterTask:
                        filePath: "{out_file}"
                        newFile: false
                        sourceMeta:
                          coordSysId: "{coord_sys_id}"
                          sourceVersion: "{source_version}"
"""

    batch_file_path = "gautil_batch_cohort.yaml"
    with open(batch_file_path, "w") as f:
        f.write(gautil_batch_content)
    print(f"Created batch cohort file: {batch_file_path}")

    manifest_file = "manifest_cohort_merge.txt"
    with open(manifest_file, "w") as f:
        for tsf in new_counts_files:
            f.write(f"{tsf}\n")

    print("Running gautil cohort merge...")
    run_process_with_filtered_output(
        [
            gautil_path, "run",
            f"--annotationFolder={annotations_folder}",
            "--manifest", manifest_file,
            "-c", batch_file_path,
        ],
        filter_warnings=["GAFeatureReader loop level greater than 1"],
    )

    # Remove the intermediate per-manifest TSFs (they are temporary and take up space)
    for tsf in new_counts_files:
        try:
            os.remove(tsf)
        except FileNotFoundError:
            pass

    print("Precomputing output file...")
    subprocess.run([gautil_path, "precompute", out_file], check=True)
    print(f"Successfully created: {out_file}")


if __name__ == "__main__":
    main()
