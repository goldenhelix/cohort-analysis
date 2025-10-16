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

## Output

- Updated cohort annotation track with merged variant frequencies
- Results stored in the user annotations folder
