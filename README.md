# Build Cohort Annotation Track

Generate or update a cohort allele frequency annotation track from VCF files using the specified cohort definition. If the cohort already exists, a new version is created with merged variant frequencies.

## Usage

### Step 1: Define Cohort (First Time Only)

If you haven't created a cohort yet, first run the **Define Cohort** task to create a cohort parameter file:

- **Cohort Name**: Name for your cohort
- **Series Name**: Name for the series
- **Sample Name Threshold**: Maximum number of sample names to list for rare variants (default: 20)
- **INFO Filter**: Filter expression for INFO fields (default: `QUAL > 10"`)
- **FORMAT Filter**: Filter expression for FORMAT fields (default: `DP > 2`)

### Step 2: Build Cohort Annotation Track

Run the **Build Cohort Annotation Track** workflow with these parameters:

1. **VCF Input Directory**: Directory containing `*.vcf.gz` files to process
2. **Cohort Parameter File**: Select the cohort parameter file created in Step 1

## Process

1. Reads variants from the input VCF files
2. Applies INFO and FORMAT field filters
3. Merges filtered variants with existing variant counts
4. Creates or updates the cohort annotation track

## Coordinate System (Assembly)

The coordinate system (`coordSysId`) written to the output track defaults to the
workspace's assembly. The server exposes this to the task as the
`GH_WORKSPACE_ASSEMBLY` environment variable, whose value is already a full
coordSysId (for example `GRCh_38,Chromosome,Homo sapiens` or
`GRCh_37_g1k,Chromosome,Homo sapiens`), and it is used as-is.

To override the assembly, add a `coord_sys_id` line to the cohort parameter file:

```
coord_sys_id=GRCh_38,Chromosome,Homo sapiens
```

When present, this value is used instead of `GH_WORKSPACE_ASSEMBLY` and is applied
via the `TsfWriterTask` `sourceMeta` (SourceMeta) transform. Keep this value
consistent across every update to a cohort — incremental updates merge new counts
into the existing track, so mixing assemblies would produce an invalid result.

## Output

- Updated cohort annotation track with merged variant frequencies
- Results stored in the user annotations folder
