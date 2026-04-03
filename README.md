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

Processing is **batched**: up to `batch_size` files share one Cromwell scatter
shard (one VM). The **default `batch_size` is `1`**, which maximises parallelism
(one input file per shard). Larger values amortise the ~98 GB dbSNP download
across more files per VM at the cost of fewer parallel shards.

## Repository layout

```
mac-filter-rsidify/
├── docker/
│   └── Dockerfile              # Python 3.11-slim + gawk + bc + gzip
├── scripts/
│   ├── rsidify_sqlite.py           # rsID annotation via SQLite
│   ├── generate_manifest.py        # Helper to create the input manifest
│   └── harvest_cromwell_outputs.py # Copy finished Cromwell outputs into chunk folders on GCS
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
The pipeline expects a **headerless, tab-delimited** manifest with four
columns: `scripts/generate_manifest.py` does all three steps in one command:

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
# Uniform sample size — all defaults (MAC 30, MAF 0.0001, batch_size 1)
python3 scripts/generate_manifest.py \
    --sumstats-dir gs://bucket/path/to/sumstats \
    --sample-size 4144 \
    --analysis-type pqtl \
    --output manifest.tsv

# Per-trait sample sizes from a mapping file
python3 scripts/generate_manifest.py \
    --sumstats-dir gs://bucket/path/to/sumstats \
    --sample-size-map sample_sizes.tsv \
    --analysis-type pqtl \
    --output manifest.tsv
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
| `--batch-size` | `1` | Files processed per VM (default maximises parallelism) |
| `--chrom-col` | `#chrom` | Chromosome column name |
| `--pos-col` | `pos` | Position column name |
| `--ref-col` | `ref` | Reference allele column name |
| `--alt-col` | `alt` | Alternative allele column name |
| `--af-col` | `af_alt` | Allele frequency column name |
| `--beta-col` | `beta` | Effect size column name |
| `--pval-col` | `pval` | P-value column name |

Use `--no-upload` or `--no-json` to suppress the corresponding side-effect
(e.g. for dry-runs or regenerating one artefact independently).
| Key | Description |
|-----|-------------|
| `sumstats_manifest` | GCS path to your manifest TSV |
| `dbsnp_database` | GCS path to the dbSNP SQLite DB |
| `docker` | Full Docker image URI with tag |

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
`/path/to/CromwellInteract/cromwell_interact.py`.

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
| `batch_size` | `Int` | `1` | Files processed per VM (default maximises parallelism) |
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

## Harvesting outputs after Cromwell finishes (standalone)

When a workflow has **already succeeded**, Cromwell keeps `*_intermediary.gz`,
`*_premunged.gz`, and per-trait `.log` files under the **execution** bucket (for
example `gs://cromwell-fg-3/mac_filter_rsidify/<workflow-uuid>/`). They are **not**
copied automatically to `gs://r13-data/fg3_aggregate_pQTLs/...`. Use
`scripts/harvest_cromwell_outputs.py` to flatten them into a chunk layout you
control.

### Task this script performs

Given a **workflow id** (and optionally the **workflow root** on GCS), the
script copies:

| Cromwell / WDL output | Destination under ``--dest`` |
|------------------------|------------------------------|
| ``intermediary_outputs`` (full-column MAC/MAF-filtered ``*_intermediary.gz``) | ``<dest>/intermediary/`` |
| ``per_file_logs`` (pipeline logs, not Cromwell VM logs) | ``<dest>/logs/`` |
| ``premunged_outputs`` (LDSC five-column ``*_premunged.gz``) | ``<dest>/mac_filtered_output/`` |

Optional ``--copy-summary`` also copies ``summary.csv`` to ``<dest>/logs/summary.csv``.

### Why not only ``gsutil ls -r``?

Under ``call-rsidify_and_filter_batch/``, paths may include **multiple**
``attempt-*`` directories after retries, and Cromwell may also expose outputs
under ``shard-N/glob-*`` **without** ``attempt-*`` in the URI. A naive recursive
copy can duplicate or resurrect stale files. The harvest script resolves the
correct object per scatter shard (see script docstring). Alternatively, use the
Cromwell **outputs** API (below), which always lists the final URIs Cromwell
registered for the workflow.

### Relationship to ``cromwell_interact.py outfiles``

``cromwell_interact.py`` ``outfiles`` calls Cromwell’s
``GET /api/workflows/v1/{id}/outputs`` over an SSH SOCKS tunnel (same as
``meta`` / ``submit``). It prints **one ``gs://`` URI per line** for each output
value, or for a single key if you pass ``-tag``:

```bash
# After: gcloud compute ssh <cromwell-vm> -- -f -n -N -D localhost:5000
# Default CromwellInteract ports: SOCKS 5000, remote Cromwell HTTP 80

python3 /path/to/CromwellInteract/cromwell_interact.py outfiles <workflow-uuid> \
  -tag mac_filter_rsidify.intermediary_outputs
```

That list is exactly the set of **intermediary** files Cromwell considers
workflow outputs. The harvest script can consume the **full** JSON payload from
the same endpoint (see option B), or you can use ``outfiles`` for a quick sanity
check (line count should match your manifest row count).

Full output keys (for ``-tag``):

* ``mac_filter_rsidify.intermediary_outputs``
* ``mac_filter_rsidify.premunged_outputs``
* ``mac_filter_rsidify.per_file_logs``
* ``mac_filter_rsidify.summary_csv``

### Walk-through

**Prerequisites:** ``gsutil`` on ``PATH``, credentials that can **read** the
Cromwell execution bucket and **write** ``--dest``. Python 3.9+.

#### Option A — GCS workflow root only (no SSH tunnel) **recommended**

Use this when you know the workflow root on the execution bucket (from Cromwell
metadata ``workflowRoot``, the UI, or ``cromwell meta <id>``). The script runs
``gsutil ls -r`` on ``.../call-rsidify_and_filter_batch/`` and selects the
correct objects per shard.

```bash
cd /path/to/mac-filter-rsidify

WF_ROOT="gs://cromwell-fg-3/mac_filter_rsidify/<workflow-uuid>"
DEST="gs://r13-data/fg3_aggregate_pQTLs/02.chunk_0501_1000"

# Inspect counts only
python3 scripts/harvest_cromwell_outputs.py \
  --gcs-workflow-root "$WF_ROOT" \
  --dest "$DEST" \
  --dry-run

# Copy everything (parallel gsutil cp; tune --workers if needed)
python3 scripts/harvest_cromwell_outputs.py \
  --gcs-workflow-root "$WF_ROOT" \
  --dest "$DEST" \
  --copy-summary \
  --workers 32
```

Resulting layout:

```text
${DEST}/intermediary/*.gz
${DEST}/mac_filtered_output/*.gz
${DEST}/logs/*.log
${DEST}/logs/summary.csv    # if --copy-summary
```

#### Option B — Saved Cromwell ``outputs`` JSON (offline copy)

With an SSH tunnel active, save the API response once:

```bash
curl -sS "http://localhost:80/api/workflows/v1/<workflow-uuid>/outputs" \
  -H "accept: application/json" \
  --socks5 localhost:5000 \
  > outputs.json
```

Then copy without talking to Cromwell again:

```bash
python3 scripts/harvest_cromwell_outputs.py \
  --outputs-json outputs.json \
  --dest "$DEST" \
  --copy-summary
```

The file may be either the full response (with an ``outputs`` key) or the inner
``outputs`` object only.

#### Option C — Live fetch from Cromwell (tunnel required)

Same curl behaviour as CromwellInteract, invoked from the script:

```bash
python3 scripts/harvest_cromwell_outputs.py \
  --workflow-id <workflow-uuid> \
  --dest "$DEST" \
  --socks-port 5000 \
  --http-port 80 \
  --copy-summary
```

### Verification

After a run, expect **one object per manifest row** for each of intermediary,
premunged, and per-trait logs (e.g. 500 for a 500-trait chunk):

```bash
gsutil ls "${DEST}/intermediary/*.gz" | wc -l
gsutil ls "${DEST}/mac_filtered_output/*.gz" | wc -l
gsutil ls "${DEST}/logs/*.log" | wc -l   # excludes summary.csv
head -5 <(gsutil cat "${DEST}/logs/summary.csv")
```

If counts are higher than expected, the prefix may already have leftover objects
from an earlier test; compare basenames to your manifest or re-run into a clean
prefix.

### Flags reference

| Flag | Purpose |
|------|---------|
| ``--gcs-workflow-root`` | Workflow root URI (``gs://.../<uuid>``); scan GCS, no tunnel |
| ``--outputs-json`` | Saved ``/outputs`` JSON |
| ``--workflow-id`` | Fetch ``/outputs`` via curl + SOCKS |
| ``--dest`` | Base ``gs://`` URI for the chunk folder (required) |
| ``--intermediary-subdir`` / ``--logs-subdir`` / ``--mac-filtered-subdir`` | Override subdirectory names (defaults: ``intermediary``, ``logs``, ``mac_filtered_output``) |
| ``--workers`` | Parallel ``gsutil cp`` jobs (default: 32) |
| ``--dry-run`` | Print planned counts only |
| ``--copy-summary`` | Copy ``summary.csv`` into ``logs/`` |
| ``--socks-port`` / ``--http-port`` | CromwellInteract-compatible defaults (5000 / 80) |

## Architecture

```
                       ┌──────────────────┐
                       │  sumstats_manifest│
                       │  (N files)        │
                       └────────┬─────────┘
                                │
                     ┌──────────▼──────────┐
                     │   create_batches     │
                     │  (split into ⌈N/B⌉  │
                     │   manifests, B =     │
                     │   batch_size)        │
                     └──────────┬──────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                  │
     ┌────────▼────────┐ ┌─────▼──────┐  ┌───────▼───────┐
     │ batch_0 (≤B     │ │ batch_1    │  │ batch_⌈N/B⌉-1 │
     │  files)         │ │ (≤B files) │  │ (≤B files)    │
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
2. **Processes** each of its files sequentially, up to `batch_size` per shard
   (rsIDify → filter → premunge)
3. **Cleans up** temp files after each file to minimise disk usage

### Why batching?

With hundreds of input files, **`batch_size = 1`** (the default) gives the
maximum number of Cromwell scatter shards: each shard downloads the database
once for a single sumstats file. If you **raise** `batch_size`, fewer VMs run in
parallel but each download of the ~98 GB database is shared across more files,
reducing total download time when you are not limited by Cromwell concurrency.

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
