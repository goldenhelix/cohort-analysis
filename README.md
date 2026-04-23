# Build Cohort Annotation Track

Generate or update a cohort allele frequency annotation track from VCF files using the specified cohort definition. If the cohort already exists, a new version is created with merged variant frequencies.

## Usage

### Step 1: Define Cohort (First Time Only)

If you haven't created a cohort yet, first run the **Define Cohort** task to create a cohort parameter file. Parameters:

- **Cohort Name**: Name for your cohort.
- **Series Name**: Name for the series.
- **Sample Name Threshold**: Maximum number of sample names to list for rare variants (default: 20).
- **Merge Step File Count**: Number of files merged per step (default: 128); tune for runner resources.
- **Minimum QUAL / DP / GQ**: Quality filters, see [Parameters](#parameters) below.
- **Include Reference-Confident Loci (gVCF)**: Controls whether gVCF reference blocks are preserved, see [Parameters](#parameters).

### Step 2: Build Cohort Annotation Track

Run the **Build Cohort Annotation Track** workflow with these parameters:

1. **VCF Input Directory**: Directory containing `*.vcf.gz` files to process.
2. **Cohort Parameter File**: Select the cohort parameter file created in Step 1.

## Parameters

Quality thresholds are typed, not free-form filter expressions. Each may be disabled by setting it to `0`. Before the merge runs, every input VCF's header is scanned to confirm it declares the FORMAT fields referenced by the active thresholds; missing declarations cause the run to fail fast with the offending files listed.

### `min_qual` (site-level) — default `10`

Records whose QUAL column is at or below this value are excluded. QUAL is a first-class VCF column (column 6), always present by spec, but may be missing (`.`) on gVCF reference-confident blocks. Those blocks are handled by the `include_reference_confident_loci` toggle below, not by this threshold. Set to `0` to disable.

### `min_dp` (per-sample) — default `3`

Samples whose FORMAT/DP is at or below this value are excluded from the allele-count tally for that record; they do not affect other samples at the same site. Raise for high-coverage cohorts or lower for exomes/targeted panels. Set to `0` to disable. `DP` must be declared in every input VCF's header or the run will fail.

### `min_gq` (per-sample) — default `20`

Samples whose FORMAT/GQ is below this value are excluded from the allele-count tally for that record. `20` is the community-standard high-confidence genotype threshold. Set to `0` to disable. `GQ` must be declared in every input VCF's header or the run will fail.

### `include_reference_confident_loci` — default `true`

Controls what happens to loci where no sample in the cohort carries a variant allele.

- **`true` (default, correct for gVCF cohorts):** those loci are kept. On gVCF inputs, they carry the "evidence of absence" signal — a position covered by every sample but with zero variant alleles is meaningfully different from a position never sequenced. Dropping them breaks allele-frequency denominators.
- **`false`:** a post-merge `any(AlleleCounts > 0)` filter drops zero-count loci. Use this for joint-called cohorts when you only want records with at least one alternate allele, or when output size is a concern and evidence-of-absence is not.

**gVCF / joint-VCF homogeneity required:** this workflow classifies each input VCF by whether it declares `##INFO=<ID=END,...>` (gVCF) or not (joint-called). A cohort that mixes both kinds is rejected up-front, because the QUAL-filter expression must guard against null QUAL on gVCF reference blocks (`END > 0 or QUAL > {min_qual}`) while joint-called VCFs do not declare `END`. Split mixed cohorts into separate runs, or set `min_qual=0` to sidestep the QUAL filter entirely.

## Process

1. Scans every input VCF's header; validates required FORMAT declarations; classifies the cohort as gVCF or joint; fails fast on mixed cohorts.
2. Reads variants from the input VCF files.
3. Applies `min_qual` (site), `min_dp` (per-sample), `min_gq` (per-sample) filters; composes the expression so that gVCF reference-confident blocks survive when `include_reference_confident_loci` is true.
4. Merges filtered variants with existing variant counts.
5. Creates or updates the cohort annotation track.

## Output

- Updated cohort annotation track with merged variant frequencies.
- Results stored in the user annotations folder.
