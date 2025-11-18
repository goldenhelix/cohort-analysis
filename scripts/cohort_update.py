#!/usr/bin/env python3
"""
Cohort Annotation Track Updater

Updates a cohort variant frequencies track by adding new samples.

This script merges variant data from new VCF files into an existing cohort variant
frequencies track. It:
1. Reads variants from the input VCF files
2. Filters variants based on INFO and FORMAT fields filters
3. Merges the filtered variants with any existing variant counts
4. Updates the cohort parameter file with the new settings

Environment Variables Required:
- WORKSPACE_DIR: Base workspace directory
- GH_WORKSPACE_ASSEMBLY: Assembly type (e.g., GRCh_37, GRCh_38)
- AGENT_CPU_CORES: Number of CPU cores to use (optional, defaults to system CPU count)
- TASK_DIR: Directory containing supporting scripts like threadCount.py (optional)
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime
from pathlib import Path
import json


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


def calculate_thread_counts(cpu_count, file_count, task_dir=None):
    """Calculate thread counts using threadCount.py script."""
    if task_dir is None:
        # Try to find threadCount.py relative to this script
        script_dir = Path(__file__).parent.parent / "tasks"
        task_dir = str(script_dir)

    thread_count_script = Path(task_dir) / "threadCount.py"

    if not thread_count_script.exists():
        print(f"Warning: threadCount.py not found at {thread_count_script}", file=sys.stderr)
        print(f"Using fallback thread calculation", file=sys.stderr)
        # Fallback calculation
        reader_threads = max(1, cpu_count // 4)
        readers_per_flattener = max(1, file_count // (cpu_count - 2))
        return reader_threads, readers_per_flattener

    reader_threads_cmd = [
        "python3", str(thread_count_script),
        "--reader-threads", "--cpu-count", str(cpu_count), str(file_count)
    ]
    readers_per_flattener_cmd = [
        "python3", str(thread_count_script),
        "--readers-per-flattener", "--cpu-count", str(cpu_count), str(file_count)
    ]

    reader_threads = int(subprocess.check_output(reader_threads_cmd).decode().strip())
    readers_per_flattener = int(subprocess.check_output(readers_per_flattener_cmd).decode().strip())

    return reader_threads, readers_per_flattener


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


def count_files_in_manifest(manifest_file):
    """Count non-empty lines in manifest file."""
    with open(manifest_file, 'r') as f:
        return sum(1 for line in f if line.strip())


def main():
    parser = argparse.ArgumentParser(
        description='Update cohort variant frequencies track by adding new samples',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--manifest-file',
        required=True,
        help='Text file containing paths to VCF files to process'
    )
    parser.add_argument(
        '--existing-counts',
        help='Optional TSF file containing existing variant counts to update'
    )
    parser.add_argument(
        '--out-file',
        required=True,
        help='The output file to create (base name)'
    )
    parser.add_argument(
        '--cohort-name',
        required=True,
        help='The name of the cohort to create'
    )
    parser.add_argument(
        '--series-name',
        required=True,
        help='The name of the series to create'
    )
    parser.add_argument(
        '--sample-name-threshold',
        type=int,
        default=20,
        help='The maximum number of sample names to list for rare variants (default: 20)'
    )
    parser.add_argument(
        '--info-filter',
        default='all( FILTER == "MLrejected" )',
        help='INFO field filter expression (default: all( FILTER == "MLrejected" ))'
    )
    parser.add_argument(
        '--format-filter',
        default='DP > 2',
        help='FORMAT field filter expression (default: DP > 2)'
    )
    parser.add_argument(
        '--skip-existing-counts',
        action='store_true',
        help='Skip using existing counts file'
    )
    parser.add_argument(
        '--gautil-path',
        default='/opt/apiserver/gautil',
        help='Path to gautil executable (default: /opt/apiserver/gautil)'
    )

    args = parser.parse_args()

    # Get required environment variables
    workspace_dir = get_env_or_error('WORKSPACE_DIR')
    gh_workspace_assembly = get_env_or_error('GH_WORKSPACE_ASSEMBLY')

    # Optional environment variables
    agent_cpu_cores = int(os.environ.get('AGENT_CPU_CORES', os.cpu_count() or 8))
    task_dir = os.environ.get('TASK_DIR', str(Path(__file__).parent.parent / "tasks"))

    # Track parameters
    source_name = f"{args.cohort_name} Variant Frequencies"
    series_name = args.series_name
    source_version = datetime.utcnow().strftime("%Y-%m-%d")

    # Output paths
    out_file_base = args.out_file.replace('.tsf', '')
    skipped_files_path = f"{out_file_base}_skipped_duplicates.txt"

    # Determine coordinate system
    if gh_workspace_assembly.startswith('GRCh_37'):
        coord_sys_id = "GRCh_37_g1k,Chromosome,Homo sapiens"
    else:
        coord_sys_id = "GRCh_38,Chromosome,Homo sapiens"

    print(f"Workspace Assembly: {gh_workspace_assembly} => {coord_sys_id}")

    # Annotations folder
    annotations_folder = os.path.join(workspace_dir, "AppData/Common Data/Annotations")

    # Determine existing counts file
    existing_counts = args.existing_counts
    if not existing_counts:
        # Find most recent output file
        import glob
        pattern = f"{out_file_base}_*.tsf"
        files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        if files:
            existing_counts = files[0]

    # Create timestamped output file
    timestamp = int(datetime.now().timestamp())
    out_file = f"{out_file_base}_{source_version}_{timestamp}.tsf"

    # Check for TBI index files
    check_tbi_files(args.manifest_file)

    # Set up environment for gautil
    os.environ['GOLDENHELIX_USERDATA'] = os.path.join(workspace_dir, 'AppData')
    crash_dump_dir = os.path.join(workspace_dir, 'AppData/VarSeq/User Data')
    if os.path.exists(crash_dump_dir):
        os.environ['GH_CRASH_DUMP_DIR'] = crash_dump_dir

    # Build filter existing section
    filter_existing_section = ""
    existing_counts_path = ""
    existing_counts_samples_path = ""

    if existing_counts and existing_counts.endswith('.tsf') and not args.skip_existing_counts:
        if os.path.exists(existing_counts):
            existing_counts_path = existing_counts
            existing_counts_samples_path = f"{existing_counts}:2"
            print(f"Using existing counts: {existing_counts_path}")

            # Generate schema files
            subprocess.run([args.gautil_path, "schema", existing_counts_path],
                         stdout=open('existing_schema.json', 'w'),
                         check=True)
            subprocess.run([args.gautil_path, "schema", existing_counts_samples_path],
                         stdout=open('existing_schema_samples.json', 'w'),
                         check=True)

            filter_existing_section = f"""- FilterFilesWithSamplesTask:
    samplesFilePath: "{existing_counts_samples_path}"
    logFile: "{skipped_files_path}"
"""

    # Build filter by expression section
    filter_by_expr_section = ""
    if args.info_filter or args.format_filter:
        filter_by_expr_section = "      - filterByExpr:\n"
        if args.info_filter:
            quoted_info_filter = quote_if_needed(args.info_filter)
            filter_by_expr_section += f"          expr: {quoted_info_filter}\n"
        if args.format_filter:
            quoted_format_filter = quote_if_needed(args.format_filter)
            filter_by_expr_section += f"          sampleExpr: {quoted_format_filter}\n"

    # Calculate thread counts
    file_count = count_files_in_manifest(args.manifest_file)
    reader_threads, readers_per_flattener = calculate_thread_counts(
        agent_cpu_cores, file_count, task_dir
    )

    print(f"CPU cores: {agent_cpu_cores}")
    print(f"File count: {file_count}")
    print(f"Reader threads: {reader_threads}")
    print(f"Readers per flattener: {readers_per_flattener}")

    # Create gautil batch file
    gautil_batch_content = f"""{filter_existing_section}- forEach:
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
    onlyMergeMatchingRefAlts: true
    mergeDifferentRecordTypes: false
    inputBufferSize: 2000
    readerWorkerThreads: {reader_threads}
    readersPerFlattener: {readers_per_flattener}

- additiveCountAlleles:
    existingCountsSource: "{existing_counts_path}"
    existingCountsSampleSource: "{existing_counts_samples_path}"
    countNoCalls: true
    sourceNamePrefix: "{source_name}"
    outputSampleNamesThreshold: {args.sample_name_threshold}

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

    batch_file_path = "gautil_batch_file.yaml"
    with open(batch_file_path, 'w') as f:
        f.write(gautil_batch_content)

    print(f"Created batch file: {batch_file_path}")

    # Run gautil
    print("Running gautil...")
    gautil_cmd = [
        args.gautil_path, "run",
        f"--annotationFolder={annotations_folder}",
        "--manifest", args.manifest_file,
        "-c", batch_file_path
    ]

    # Filter out specific warning messages
    process = subprocess.Popen(
        gautil_cmd,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True
    )

    # Stream output
    for line in process.stdout:
        print(line, end='')

    for line in process.stderr:
        if "GAFeatureReader loop level greater than 1" not in line:
            print(line, end='', file=sys.stderr)

    return_code = process.wait()
    if return_code != 0:
        print(f"Error: gautil failed with return code {return_code}", file=sys.stderr)
        sys.exit(return_code)

    # Precompute the output file
    print("Precomputing output file...")
    subprocess.run([args.gautil_path, "precompute", out_file], check=True)

    # Clean up empty skipped files
    if os.path.exists(skipped_files_path) and os.path.getsize(skipped_files_path) == 0:
        os.remove(skipped_files_path)
        print(f"Removed empty skipped files log: {skipped_files_path}")

    print(f"Successfully created: {out_file}")


if __name__ == '__main__':
    main()
