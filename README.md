# MAC Filter with RSIDify — WDL Pipeline

A [WDL](https://openwdl.org/) workflow for FinnGen Refinery (Cromwell on GCP)
that processes GWAS summary statistics through **rsID annotation**, **MAC/MAF
filtering**, and **LDSC pre-munging** at scale.

## What it does

For every input summary-statistics `.gz` file the pipeline runs three stages:

| Stage | Description | Output |
|-------|-------------|--------|
| **1. RSIDify** | Annotates each variant with an rsID by querying a ~98 GB dbSNP SQLite database on `(chrom, pos, ref, alt)` | temp file with `rsid` column prepended |
| **2. MAC/MAF filter** | Computes `MAF = min(af, 1-af)` and `MAC = MAF × sample_size`; rejects variants below the configurable thresholds | `*_intermediary.gz` — all original columns + rsid, filtered |
| **3. LDSC premunge** | Extracts `SNP, A1, A2, BETA, P`; explodes comma-separated rsIDs into separate rows | `*_premunged.gz` — 5-column LDSC format |

Processing is **batched**: `batch_size` files (default 10) share a single VM so
the large dbSNP database is downloaded only once per batch rather than once per
file.

## Repository layout

```
mac-filter-rsidify/
├── docker/
│   └── Dockerfile              # Python 3.11-slim + gawk + bc + gzip
├── scripts/
│   ├── rsidify_sqlite.py       # rsID annotation via SQLite
│   └── generate_manifest.py    # Helper to create the input manifest
├── wdl/
│   ├── mac_filter_rsidify.wdl  # Main workflow
│   └── mac_filter_rsidify.json # Example Refinery inputs
├── requirements.txt
└── README.md
```

## Prerequisites

* **Refinery / Cromwell** with GCP backend (europe-west1 recommended)
* **dbSNP SQLite database** uploaded to GCS  
  Default: `gs://r13-data/reza/dbsnp_b155/dbsnp_b155_hg38.db`  
  The database must contain a `variants` table with columns
  `(chrom TEXT, pos INTEGER, ref TEXT, alt TEXT, rsid TEXT)` and an
  index on `(chrom, pos, ref, alt)`.
* **Docker image** pushed to the Artifact Registry (see [Building the Docker
  image](#building-the-docker-image) below).

## Quickstart

### 1. Generate the manifest

The pipeline expects a **headerless, tab-delimited** manifest with four
columns:

```
trait_name    analysis_type    gs://path/to/sumstats.gz    sample_size
```

Use the helper script:

```bash
# Uniform sample size
python3 scripts/generate_manifest.py \
    --sumstats-dir gs://bucket/path/to/sumstats \
    --sample-size 4144 \
    --analysis-type pqtl \
    --output manifest.tsv

# Per-trait sample sizes
python3 scripts/generate_manifest.py \
    --sumstats-dir gs://bucket/path/to/sumstats \
    --sample-size-map sample_sizes.tsv \
    --analysis-type pqtl \
    --output manifest.tsv
```

Upload the manifest to GCS:

```bash
gsutil cp manifest.tsv gs://your-bucket/path/to/manifest.tsv
```

### 2. Edit the inputs JSON

Copy `wdl/mac_filter_rsidify.json` and set at minimum:

| Key | Description |
|-----|-------------|
| `sumstats_manifest` | GCS path to your manifest TSV |
| `dbsnp_database` | GCS path to the dbSNP SQLite DB |
| `docker` | Full Docker image URI with tag |

All other parameters have sensible defaults.

### 3. Submit to Refinery

Upload the WDL and inputs JSON to Refinery, or submit via the Cromwell API:

```bash
# Example using cromshell
cromshell submit wdl/mac_filter_rsidify.wdl inputs.json
```

## WDL Inputs Reference

### Required

| Input | Type | Description |
|-------|------|-------------|
| `sumstats_manifest` | `File` | Headerless TSV: `trait \t analysis_type \t gs://path \t sample_size` |
| `dbsnp_database` | `File` | GCS path to the dbSNP SQLite database (~98 GB) |
| `docker` | `String` | Docker image URI |

### Optional (with defaults)

| Input | Type | Default | Description |
|-------|------|---------|-------------|
| `mac_threshold` | `Int` | `30` | Minimum Minor Allele Count |
| `maf_threshold` | `Float` | `0.0001` | Minimum Minor Allele Frequency |
| `batch_size` | `Int` | `10` | Files processed per VM |
| `chrom_col` | `String` | `#chrom` | Chromosome column name in input |
| `pos_col` | `String` | `pos` | Position column name |
| `ref_col` | `String` | `ref` | Reference allele column name |
| `alt_col` | `String` | `alt` | Alternative allele column name |
| `af_col` | `String` | `af_alt` | Allele frequency column name |
| `beta_col` | `String` | `beta` | Effect size column name |
| `pval_col` | `String` | `pval` | P-value column name |

## Outputs

| Output | Type | Description |
|--------|------|-------------|
| `premunged_outputs` | `Array[File]` | LDSC-format `.gz` files (SNP, A1, A2, BETA, P) |
| `intermediary_outputs` | `Array[File]` | Full-column filtered `.gz` files (all columns + rsid) |
| `per_file_logs` | `Array[File]` | Per-file processing logs with variant counts |
| `summary_csv` | `File` | Aggregated CSV summary of all processed files |

## Architecture

```
                       ┌──────────────────┐
                       │  sumstats_manifest│
                       │  (N files)        │
                       └────────┬─────────┘
                                │
                     ┌──────────▼──────────┐
                     │   create_batches     │
                     │  (split into N/10    │
                     │   batch manifests)   │
                     └──────────┬──────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                  │
     ┌────────▼────────┐ ┌─────▼──────┐  ┌───────▼───────┐
     │ batch_0 (10 files│ │ batch_1    │  │ batch_N/10    │
     │                  │ │ (10 files) │  │ (≤10 files)   │
     │ 1. rsIDify       │ │            │  │               │
     │ 2. MAC/MAF filter│ │   ...      │  │    ...        │
     │ 3. LDSC premunge │ │            │  │               │
     └────────┬────────┘ └─────┬──────┘  └───────┬───────┘
              │                │                  │
              └────────────────┼──────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   gather_summary     │
                    │  (merge all logs     │
                    │   into summary.csv)  │
                    └─────────────────────┘
```

Each scattered batch VM:
1. **Localises** the ~98 GB dbSNP database once (Cromwell handles this;
   ~3–5 min within europe-west1 at 3–5 Gbps intra-region bandwidth)
2. **Processes** each of its 10 files sequentially (rsIDify → filter → premunge)
3. **Cleans up** temp files after each file to minimise disk usage

### Why batching?

With hundreds of input files, a naive one-file-per-VM scatter would create
hundreds of VMs each downloading the 98 GB database.  Batching into groups of
10 reduces the number of database downloads by 10×, saving ~3–5 min of startup
overhead per eliminated VM while keeping parallelism high.

### Runtime characteristics per batch VM

| Resource | Value | Rationale |
|----------|-------|-----------|
| CPU | 4 | rsIDify is single-threaded; extra cores for parallel I/O compression |
| Memory | 16 GB | SQLite page-cache warm-up benefits from available RAM; handles larger batches |
| Disk | ~128 GB SSD | 98 GB DB + 30 GB working space; SSD for random-access I/O |
| Preemptible | 2 retries | Cost-effective; Cromwell retries on preemption |

## Building the Docker image

From the repository root:

```bash
docker build \
    -f docker/Dockerfile \
    -t europe-west1-docker.pkg.dev/finngen-refinery-dev/fg-refinery-registry/mac-filter-rsidify:v1 \
    .

docker push \
    europe-west1-docker.pkg.dev/finngen-refinery-dev/fg-refinery-registry/mac-filter-rsidify:v1
```

The image is lightweight (~150 MB):
- `python:3.11-slim` base
- `gawk`, `bc`, `gzip` system packages
- `scripts/rsidify_sqlite.py` (uses only Python stdlib: `sqlite3`, `gzip`,
  `argparse`)

## Error handling

- Each file in a batch is processed inside a subshell.  If one file fails, the
  rest of the batch continues.
- Failed files are logged with `status=FAILED` and appear in `summary.csv`.
- The WDL task itself succeeds even if individual files fail (partial results
  are still collected via `glob()`).  Check `summary_csv` for failures.
- Cromwell retries preempted VMs automatically (up to `preemptible` count).

## Relationship to existing scripts

This pipeline is the cloud-native replacement for the local parallel scripts:

| Local script | WDL equivalent |
|-------------|----------------|
| `03.long_gwas_rg_LDSC/utils/munge_sumstats_sqlite_mac.sh` | `rsidify_and_filter_batch` task (steps 1–3) |
| `03.long_gwas_rg_LDSC/utils/parallel_mac_filtering.sh` | `mac_filter_rsidify` workflow scatter |
| `11.fg3_Proteomics/.../run_parallel_mac_filtering_with_progress.sh` | Full workflow (GCS I/O + batching + progress) |
| `03.long_gwas_rg_LDSC/utils/rsidify_sqlite.py` | `scripts/rsidify_sqlite.py` (adapted, no hardcoded paths) |
