# Cohort Analysis Workflow

A workflow for analyzing variant frequencies across multiple samples using VCF files.

## Overview

This workflow has two main steps:
1. Creates a manifest of VCF files
2. Processes the VCF files to generate variant frequencies

## Usage

1. Place your VCF.gz files in a directory
2. Run the workflow with:
   - Input directory: where your VCF files are
   - Output directory: where results will be saved

## Output

- A manifest file listing all processed VCF files
- A TSF file containing variant frequencies

## Requirements

- VCF files must be compressed (.vcf.gz)
- VCF files must contain standard fields (DP, GQ, AD)
- Appropriate read/write permissions
