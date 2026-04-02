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

### 1. Generate the manifest, upload it, and create the inputs JSON

`scripts/generate_manifest.py` does all three steps in one command:

1. Discovers `.gz` sumstats in `--sumstats-dir`
2. Writes a manifest TSV locally at `--output`
3. Uploads the TSV to `<sumstats-dir>/<tsv-basename>` on GCS
4. Writes a Cromwell inputs JSON beside the TSV (same stem, `.json` extension),
   with `sumstats_manifest` pointing at the uploaded GCS path

The pipeline expects a **headerless, tab-delimited** manifest with four columns:

```
trait_name    analysis_type    gs://path/to/sumstats.gz    sample_size
```

```bash
# Uniform sample size — all defaults (MAC 30, MAF 0.0001, batch_size 10)
python3 scripts/generate_manifest.py \
    --sumstats-dir gs://r13-data/fg3_aggregate_pQTLs/05.chunk_2001_2500/ \
    --sample-size 4144 \
    --analysis-type pqtl \
    --output wdl/05.chunk_2001_2500.tsv

# Per-trait sample sizes from a mapping file
python3 scripts/generate_manifest.py \
    --sumstats-dir gs://bucket/path/to/sumstats \
    --sample-size-map sample_sizes.tsv \
    --analysis-type pqtl \
    --output wdl/05.chunk_2001_2500.tsv
```

The mapping file (`--sample-size-map`) is a headerless TSV with columns
`trait_name` and `sample_size`.

**All WDL pipeline parameters are configurable** and default to the FinnGen
production values:

| Flag | Default | Description |
|------|---------|-------------|
| `--dbsnp-database` | `gs://r13-data/reza/dbsnp_b155/dbsnp_b155_hg38.db` | dbSNP SQLite DB |
| `--docker` | `…/mac-filter-rsidify:v1` | Docker image URI |
| `--mac-threshold` | `30` | Minimum Minor Allele Count |
| `--maf-threshold` | `0.0001` | Minimum Minor Allele Frequency |
| `--batch-size` | `10` | Files processed per VM |
| `--chrom-col` | `#chrom` | Chromosome column name |
| `--pos-col` | `pos` | Position column name |
| `--ref-col` | `ref` | Reference allele column name |
| `--alt-col` | `alt` | Alternative allele column name |
| `--af-col` | `af_alt` | Allele frequency column name |
| `--beta-col` | `beta` | Effect size column name |
| `--pval-col` | `pval` | P-value column name |

Use `--no-upload` or `--no-json` to suppress the corresponding side-effect
(e.g. for dry-runs or regenerating one artefact independently).

### 2. Submit to Refinery via `cromwell_interact.py`

After step 1, submit the generated JSON directly:

```bash
cromwell submit \
    --wdl wdl/mac_filter_rsidify.wdl \
    --inputs wdl/05.chunk_2001_2500.json \
    --l product=core-analysis-r13

# Monitor progress
cromwell meta <workflow-id> -r          # quick status
cromwell meta <workflow-id> -s          # full scatter summary
```

Where `cromwell` is an alias for
`/path/to/CromwellInteract/cromwell_interact.py` (see your `~/.bash_profile`).
An SSH SOCKS tunnel must be open beforehand (`cromwell-fg-refinery2` or
`cromwell-fg-3` alias).

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

### Where results are written on GCS (important)

Cromwell does **not** write pipeline outputs back into the same bucket or folder
as your input summary statistics unless you add a separate copy step.  Inputs
are **localised** from paths in the manifest (e.g. `gs://r13-data/.../trait.gz`);
**outputs** are **delocalised** to the Cromwell **execution** bucket for that
workflow run.

#### Workflow execution root

After submission, each run has a unique id.  Cromwell stores everything under a
**workflow root** whose exact bucket depends on your Refinery / Cromwell
backend configuration.  Typical FinnGen patterns include:

| Backend / environment | Example workflow root prefix |
|-------------------------|------------------------------|
| Some Refinery deployments | `gs://cromwell-fg-3/mac_filter_rsidify/<workflow-uuid>/` |
| Others (e.g. Refinery 2) | `gs://fg-cromwell-refinery2/mac_filter_rsidify/<workflow-uuid>/` |

Replace `<workflow-uuid>` with the id returned by Cromwell when you submit (also
recorded in `CromwellInteract/workflows.log` if you use `cromwell_interact.py`).
The **authoritative** path for a given run appears in Cromwell metadata as
`workflowRoot`, or in the UI / `cromwell meta <id> -s` output.

#### Per-batch (`rsidify_and_filter_batch`) outputs

For each scatter shard (one per batch of `batch_size` traits), Cromwell writes
task outputs under:

```text
<workflowRoot>/call-rsidify_and_filter_batch/shard-<N>/glob-<hash>/
```

There are **three** `glob()` outputs from the WDL, so you will normally see **three**
different `glob-<hash>/` directories per shard (Cromwell hashes may differ per
output expression).  Files include:

| Pattern | Content |
|---------|---------|
| `*_<analysis>_mac<MAC>_maf<MAF>_premunged.gz` | LDSC five-column premunged sumstats |
| `*_<analysis>_mac<MAC>_maf<MAF>_intermediary.gz` | Full columns + `rsid`, MAC/MAF filtered |
| `*_<analysis>.log` | Per-trait processing log (variant counts, duration, SUCCESS/FAILED) |

The MAF segment in filenames may appear in scientific notation (e.g.
`maf1.0E-4`) depending on floating-point formatting.

**Nothing appears under these `glob-*` paths until that shard’s task completes
successfully.**  While a VM is still running (or retrying after preemption),
only logs such as `stdout`, `stderr`, and GCP Batch agent logs may be present.

#### `gather_summary` output

After **all** scatter shards of `rsidify_and_filter_batch` finish, Cromwell runs
`gather_summary`.  The aggregated file is written under:

```text
<workflowRoot>/call-gather_summary/
```

(Exact object name is resolved by Cromwell; inspect that call’s outputs in
metadata or use `cromwell outfiles <id>` — see below.)

#### What stays in your “input” bucket

Paths you put in the manifest (and any manifest TSV you uploaded next to your
chunk) remain **inputs only**.  Do not expect `*_premunged.gz` or
`*_intermediary.gz` to appear automatically under
`gs://r13-data/fg3_aggregate_pQTLs/...` unless you **copy** them there after the
run.

#### Finding files in practice

```bash
# List all premunged outputs for one workflow (adjust bucket and uuid)
gsutil ls -r "gs://cromwell-fg-3/mac_filter_rsidify/<workflow-uuid>/call-rsidify_and_filter_batch/" | grep premunged

# Cromwell metadata: print workflowRoot and output file paths
# (requires an SSH tunnel and cromwell_interact.py, or the Cromwell REST API)
python3 /path/to/CromwellInteract/cromwell_interact.py outfiles <workflow-uuid>
```

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
