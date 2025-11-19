#!/usr/bin/env python3
"""
Cohort Annotation Track Updater
"""
import glob
import os
import sys
import subprocess
import argparse
from datetime import datetime
from pathlib import Path
import json

from cohort_utils import calculate_thread_counts, get_env_or_error, quote_if_needed, run_process_with_filtered_output, load_config_file

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
        '--manifest-parameter-file',
        required=True,
        help='Path to the manifest parameter file containing VCF file paths'
    )
    parser.add_argument(
        '--existing-counts',
        required=False,
        help='Path to the existing counts file'
    )
    parser.add_argument(
        '--out-file',
        required=False,
        help='Path to the output file'
    )

    args = parser.parse_args()

    # Load configuration from file
    config = load_config_file(args.config)
    manifest_parameter_file = args.manifest_parameter_file

    print(f"Loading manifest parameter file: {manifest_parameter_file}")
    new_counts_files = []

    manifest_file_base = os.path.basename(manifest_parameter_file)
    with open(manifest_parameter_file, 'r') as f:
      header = f.readline()
      print(f"Header: {header}")
      for line in f:
        line = line.strip()
        if not line or line.startswith('#'):
          continue

        output_file = line.split(',')[0]
        output_file = output_file.replace('.manifest.txt', '.tsf')

        output_file_path = os.path.join(os.path.dirname(manifest_parameter_file), output_file)
        new_counts_files.append(output_file_path)
        print(f"New counts file: {output_file_path}")

    if len(new_counts_files) == 0:
      print("No new processed files found")
      sys.exit(1)

    #
    cohort_name = config['cohort_name']
    series_name = config['series_name']
    source_version = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")

    # Get required environment variables
    workspace_dir = get_env_or_error('WORKSPACE_DIR')
    gh_workspace_assembly = get_env_or_error('GH_WORKSPACE_ASSEMBLY')

    # Optional environment variables
    agent_cpu_cores = int(os.environ.get('AGENT_CPU_CORES'))
    agent_memory_gb = int(os.environ.get('AGENT_MEMORY_GB'))
    task_dir = os.environ.get('TASK_DIR')


    out_file = args.out_file
    if not out_file:
        out_file = config['out_file']
    if out_file.endswith('.tsf'):
        out_file = out_file[:-4]
    
    if not args.out_file:
      out_file = f"{out_file}_{source_version}"

    if not out_file.endswith('.tsf'):
      out_file = f"{out_file}.tsf"

    out_file = os.path.join(workspace_dir, out_file)
    

    # Find the existing counts
    if args.existing_counts:
        existing_counts = args.existing_counts
    else:
        # Look for the most recent counts file
        existing_counts = f"{out_file}_*.tsf"
        existing_counts = glob.glob(existing_counts)
        if existing_counts:
            # sort the files by name which should be the same as the source version
            existing_counts = sorted(existing_counts, key=os.path.basename)
            existing_counts = existing_counts[-1]
        else:
            existing_counts = None
    
    existing_counts_samples = None
    if existing_counts:
      print(f"Using existing counts: {existing_counts}")
      existing_counts_samples = f"{existing_counts}:2"
    else:
      print("No existing counts file found")
      existing_counts = ""
      existing_counts_samples = ""


    sample_name_threshold = int(config.get('sample_name_threshold', '20'))
    info_filter = config.get('info_filter')
    format_filter = config.get('format_filter')

    gautil_path = os.environ.get('GAUTIL_PATH', '/opt/apiserver/gautil')

    # Track parameters
    source_name = f"{cohort_name} Variant Frequencies"

    # Determine coordinate system
    if gh_workspace_assembly.startswith('GRCh_37'):
        coord_sys_id = "GRCh_37_g1k,Chromosome,Homo sapiens"
    else:
        coord_sys_id = "GRCh_38,Chromosome,Homo sapiens"

    print(f"Workspace Assembly: {gh_workspace_assembly} => {coord_sys_id}")

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

    print(f"CPU cores: {agent_cpu_cores}")
    print(f"Memory (GB): {agent_memory_gb}")

    reader_threads, readers_per_flattener = calculate_thread_counts(agent_cpu_cores, len(new_counts_files))

    print(f"Reader threads: {reader_threads}")
    print(f"Readers per flattener: {readers_per_flattener}")

    # Create gautil batch file
    gautil_batch_content = f"""
        - mergeVariantsTransform:
            onlyMergeMatchingRefAlts: true
            mergeDifferentRecordTypes: false
            inputBufferSize: 100
            readerWorkerThreads: {reader_threads}
            readersPerFlattener: {readers_per_flattener}

        - additiveCountAlleles:
            existingCountsSource: "{existing_counts}"
            existingCountsSampleSource: "{existing_counts_samples}"
            countNoCalls: true
            sourceNamePrefix: "{source_name}"
            outputSampleNamesThreshold: {sample_name_threshold}

        - runTaskLists:
            taskLists:
              - SourceTaskListTask:
                  taskList:
                    - createAnnotation
                    - TsfWriterTask:
                        filePath: "{out_file}"
                        sourceMeta:
                          coordSysId: "{coord_sys_id}"
                          seriesName: "{series_name}"
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
    with open(batch_file_path, 'w') as f:
        f.write(gautil_batch_content)

    print(f"Created batch cohort file: {batch_file_path}")

    # Create a manifest file for the new counts files
    manifest_file = f"manifest_cohort_merge.txt"
    with open(manifest_file, 'w') as f:
      for new_counts_file in new_counts_files:
        f.write(f"{new_counts_file}\n")

    # Run gautil
    print("Running gautil cohort merge...")
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
    print("Precomputing output file...")
    subprocess.run([gautil_path, "precompute", out_file], check=True)

    print(f"Successfully created: {out_file}")


if __name__ == '__main__':
    main()
