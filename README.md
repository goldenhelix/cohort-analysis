# Build Cohort Annotation Track

Generate or update a cohort allele-frequency annotation track from VCF files. If you select an existing cohort TSF, a new version is produced with the new samples' counts added; otherwise a new cohort is built from scratch.

## Usage

Run the **Build Cohort Annotation Track** workflow and fill in the form. There is no separate "define cohort" step — every tunable value is on the workflow's run form, with defaults.

### Primary fields

- **VCF Input Directory** *(required)* — folder of VCF files to add to the cohort (`.vcf.gz`, `.gvcf.gz`, `.vcf`, `.gvcf`). Scanned recursively; files whose samples are already present in the selected cohort are skipped.
- **Cohort Name** — display name for the cohort. Required for new cohorts. **Ignored** when an existing cohort TSF is selected — the metadata from the selected TSF is used and the override is announced in the run log.
- **Series Name** — identifier used as the cohort TSF's filename stem and `seriesName` metadata. If you leave it blank when starting a new cohort, it's auto-derived by slugifying the cohort name (lowercase, spaces→underscores). Same override rule as above when an existing TSF is picked.
- **Existing Cohort TSF** *(optional)* — pick a prior cohort TSF to extend. When set, it's the source of truth for cohort identity. When blank, the workflow will also try to auto-discover the latest TSF matching the typed series name; if found, its identity is used (and logged). If neither is present or found, a new cohort is created.

### Filtering (group)

Typed thresholds replace the old free-form filter expressions. Each can be disabled by setting it to `0`. Before the merge, every input VCF's header is scanned to confirm it declares the FORMAT fields referenced by the active thresholds; missing declarations cause the run to fail fast with the offending files listed.

- **Minimum QUAL** — default `10`. Site-level QUAL floor. `QUAL` is always present by VCF spec; gVCF reference blocks with `QUAL=.` are handled via the reference-confident-loci toggle below.
- **Minimum DP** — default `3`. Per-sample depth floor. Samples below it are excluded from the allele-count tally for that record (without affecting other samples at the same site). Must be declared in FORMAT.
- **Minimum GQ** — default `10`. Per-sample genotype-quality floor. `10` retains enough data for meaningful cohort denominators; raise toward GATK's `20` for high-confidence-only cohorts. Must be declared in FORMAT.
- **Include Reference-Confident Loci (gVCF)** — default `true`. When true, loci where no sample in the cohort has a variant allele are kept. On gVCF inputs this preserves the evidence-of-absence signal — a position with 0/100 alt alleles is meaningfully different from a position never sequenced. When false, a post-merge `any(AlleleCounts > 0)` filter drops zero-count loci.

### Advanced Options (group)

- **Sample Name Threshold** — default `20`. Max sample names to list in per-variant metadata for rare variants.
- **Files Per Merge Batch** — default `128`. Batch size for the per-manifest merge step; tune for runner memory.
- **Output File Override** — optional. Workspace-relative path for the output TSF. Defaults to `AppData/Common Data/UserAnnotations/cohorts/{series_name}_{timestamp}.tsf`.

## gVCF and joint-VCF cohorts

The workflow classifies each input VCF as **gVCF** (declares `##INFO=<ID=END,...>`) or **joint-called** (does not). Cohorts must be homogeneous — mixing gVCFs and joint VCFs in one run is rejected up-front with a per-category file list. This is required because the QUAL-filter expression must short-circuit on `END` for gVCFs (to preserve reference-confident blocks with null QUAL) but cannot reference `END` on joint VCFs (which don't declare it). Split mixed inputs into separate cohorts, or set `min_qual=0` to disable the QUAL filter entirely.

## Process

1. **Manifest stage** — resolves cohort identity (typed values vs. TSF metadata, with loud override logging), reads existing sample list from the selected TSF, scans the input directory for new-sample VCFs, validates their headers, classifies the cohort, writes per-batch manifests.
2. **Per-manifest merge** — filters variants and builds an intermediate TSF for each batch.
3. **Final merge** — combines the intermediate TSFs into the cohort annotation track via gautil's `additiveCountAlleles`; applies the zero-count filter if `include_reference_confident_loci` is false; precomputes the output.

## Output

- Cohort annotation track at `AppData/Common Data/UserAnnotations/cohorts/{series_name}_{YYYY-MM-DD-HH-MM}.tsf` (or the override path).
- Existing older versions of the same cohort remain on disk for provenance.
