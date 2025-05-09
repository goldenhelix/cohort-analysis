# Cohort Analysis Workflow

This repository contains Golden Helix server tasks for analyzing variant frequencies across multiple samples using VCF files.

## Prerequisites

- VCF files must be compressed (.vcf.gz)
- VCF files must contain standard fields (DP, GQ, AD)
- Appropriate read/write permissions

## Tasks

The repository contains the following tasks:

### Create Cohort (`cohort_create.task.yaml`)

Creates initial settings for a new cohort variant frequencies track.

Parameters:
- `cohort_name`: The name of the cohort to create
- `series_name`: The name of the series to create
- `min_depth`: Minimum read depth required for variant calls (default: 10)
- `min_quality`: Minimum quality score required for variant calls (default: 7)
- `min_alt_reads`: Minimum number of alternative allele reads required (default: 2)

### Add Samples to Cohort (`cohort_update.task.yaml`)

Updates a cohort variant frequencies track by adding new samples.

Parameters:
- `cohort_name`: The name of the cohort to update
- `series_name`: The name of the series
- `manifest_file`: Text file containing paths to VCF files to process
- `existing_counts`: Optional TSF file containing existing variant counts to update
- `min_depth`: Minimum read depth required for variant calls (default: 10)
- `min_quality`: Minimum quality score required for variant calls (default: 7)
- `min_alt_reads`: Minimum number of alternative allele reads required (default: 2)
- `out_file`: The output TSF file to create

### VCF Manifest Generator (`cohort_manifest.task.yaml`)

Creates a manifest file listing all VCF.gz files from an input directory.

Parameters:
- `input_directory`: Directory containing VCF.gz files to process

## Workflows

### Add Samples to Cohort Variant Frequencies (`cohorts.workflow.yaml`)

This workflow combines the above tasks to add samples to a cohort variant frequencies track.

Process Overview:
1. Generates a manifest file listing all VCF files in the input directory
2. Reads variants from the input VCF files
3. Filters variants based on quality parameters:
   - Minimum read depth
   - Minimum quality score
   - Minimum alternative allele reads
4. Merges the filtered variants with any existing variant counts
5. Updates the cohort parameter file with the new settings

## Usage

1. Create a new cohort:
   - Run the Create Cohort task to set up initial parameters
   - Specify cohort name, series name, and quality thresholds

2. Add samples to an existing cohort:
   - Use the "Add Samples to Cohort Variant Frequencies" workflow
   - Select your existing cohort parameter file
   - Select the folder containing your new VCF files

## Output

- A manifest file listing all processed VCF files
- A TSF file containing variant frequencies
- Updated cohort parameter file with new settings

## Notes

- The workflow requires significant resources (8 CPU cores, 16GB memory)
- VCF files must be properly formatted and compressed
- The workflow automatically handles duplicate samples
- Results are stored in the user annotations folder
