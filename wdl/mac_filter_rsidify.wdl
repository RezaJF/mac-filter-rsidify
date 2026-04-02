version 1.0

# =============================================================================
# MAC Filtering with RSIDify Pipeline
# =============================================================================
#
# Processes GWAS summary statistics through:
#   1. rsID annotation via dbSNP SQLite lookup (rsidify_sqlite.py)
#   2. MAC (Minor Allele Count) and MAF (Minor Allele Frequency) filtering
#   3. LDSC pre-munging — extract SNP, A1, A2, BETA, P columns
#
# Summary statistics are processed in batches of `batch_size` per Cromwell shard
# (default 1 = one file per shard, maximum parallelism). The ~98 GB dbSNP
# database is localised once per shard; larger batch_size shares that download
# across more files per VM.
#
# Designed for FinnGen Refinery (Cromwell on GCP).
# =============================================================================

workflow mac_filter_rsidify {

  input {
    File    sumstats_manifest   # Headerless TSV: trait \t analysis_type \t gs://path \t sample_size
    File    dbsnp_database      # gs:// path to dbSNP SQLite database (~98 GB)

    Int     mac_threshold  = 30
    Float   maf_threshold  = 0.0001
    Int     batch_size     = 1

    String  docker

    # Configurable column names for input summary statistics
    String  chrom_col = "#chrom"
    String  pos_col   = "pos"
    String  ref_col   = "ref"
    String  alt_col   = "alt"
    String  af_col    = "af_alt"
    String  beta_col  = "beta"
    String  pval_col  = "pval"
  }

  # ---- Phase 1: split manifest into batches of batch_size ----
  call create_batches {
    input:
      manifest   = sumstats_manifest,
      batch_size = batch_size,
      docker     = docker
  }

  # ---- Phase 2: scatter processing over batches ----
  scatter (i in range(length(create_batches.batch_manifests))) {
    call rsidify_and_filter_batch {
      input:
        batch_manifest   = create_batches.batch_manifests[i],
        batch_paths_list = create_batches.batch_path_lists[i],
        dbsnp_db         = dbsnp_database,
        mac_threshold    = mac_threshold,
        maf_threshold    = maf_threshold,
        chrom_col        = chrom_col,
        pos_col          = pos_col,
        ref_col          = ref_col,
        alt_col          = alt_col,
        af_col           = af_col,
        beta_col         = beta_col,
        pval_col         = pval_col,
        docker           = docker
    }
  }

  # ---- Phase 3: aggregate logs into a single summary CSV ----
  Array[File] all_premunged    = flatten(rsidify_and_filter_batch.premunged_files)
  Array[File] all_intermediary = flatten(rsidify_and_filter_batch.intermediary_files)
  Array[File] all_logs         = flatten(rsidify_and_filter_batch.log_files)

  call gather_summary {
    input:
      log_files = all_logs,
      docker    = docker
  }

  output {
    Array[File] premunged_outputs    = all_premunged
    Array[File] intermediary_outputs = all_intermediary
    Array[File] per_file_logs        = all_logs
    File        summary_csv          = gather_summary.summary
  }
}


# =============================================================================
# Task: create_batches
# Splits the input manifest into chunks of batch_size.  For each batch two
# files are written:
#   batch_NNNN.tsv   — full metadata rows (trait, analysis_type, path, N)
#   paths_NNNN.txt   — one gs:// path per line (for Cromwell file localisation)
# =============================================================================

task create_batches {

  input {
    File   manifest
    Int    batch_size
    String docker
  }

  command <<<
    python3 <<'PYEOF'
import math, sys

with open("~{manifest}", "r") as fh:
    lines = [l.strip() for l in fh if l.strip()]

if not lines:
    print("Error: manifest is empty", file=sys.stderr)
    sys.exit(1)

# Skip an accidental header row
if lines[0].lower().startswith("trait\t"):
    lines = lines[1:]

total = len(lines)
bs    = ~{batch_size}
n     = math.ceil(total / bs)

for i in range(n):
    chunk = lines[i * bs : (i + 1) * bs]

    with open(f"batch_{i:04d}.tsv", "w") as mf, \
         open(f"paths_{i:04d}.txt", "w") as pf:
        for row in chunk:
            mf.write(row + "\n")
            cols = row.split("\t")
            if len(cols) < 4:
                print(f"Warning: malformed row — {row}", file=sys.stderr)
                continue
            pf.write(cols[2] + "\n")

print(f"Created {n} batches from {total} entries (batch_size={bs})", flush=True)
PYEOF
  >>>

  output {
    Array[File] batch_manifests = glob("batch_*.tsv")
    Array[File] batch_path_lists = glob("paths_*.txt")
  }

  runtime {
    docker: docker
    cpu: 1
    memory: "1 GB"
    disks: "local-disk 10 HDD"
    zones: "europe-west1-b europe-west1-c europe-west1-d"
    preemptible: 2
    noAddress: true
  }
}


# =============================================================================
# Task: rsidify_and_filter_batch
# Processes a batch of summary-statistics files sequentially on one VM.
# The ~98 GB dbSNP database is localised once per VM by Cromwell.
#
# Per file:
#   1. rsIDify via rsidify_sqlite.py (prepend rsid column)
#   2. MAC/MAF filter → intermediary (all columns retained)
#   3. Extract LDSC columns + explode multi-rsIDs → premunged
#   4. Write per-file log with variant counts
# =============================================================================

task rsidify_and_filter_batch {

  input {
    File   batch_manifest
    File   batch_paths_list
    File   dbsnp_db
    Int    mac_threshold
    Float  maf_threshold
    String chrom_col
    String pos_col
    String ref_col
    String alt_col
    String af_col
    String beta_col
    String pval_col
    String docker
  }

  # Cromwell localises these files from the gs:// paths in batch_paths_list
  Array[File] sumstats_files = read_lines(batch_paths_list)

  Int db_size   = ceil(size(dbsnp_db, "GB"))
  Int disk_size = db_size + 30

  command <<<
    set -uo pipefail

    mkdir -p outputs/premunged outputs/intermediary outputs/logs

    # Build an ordered array of localised sumstats paths
    mapfile -t LOCAL_FILES < ~{write_lines(sumstats_files)}

    batch_failed=0
    line_idx=0

    while IFS=$'\t' read -r trait analysis_type sumstats_gs_path sample_size; do
      local_input="${LOCAL_FILES[${line_idx}]}"
      ((line_idx++)) || true

      log_file="outputs/logs/${trait}_${analysis_type}.log"
      start_epoch=$(date +%s)

      echo "============================================"
      echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Processing: ${trait} (${analysis_type})"
      echo "  Localised input: ${local_input}"
      echo "  Sample size:     ${sample_size}"
      echo "  MAC >= ~{mac_threshold}, MAF >= ~{maf_threshold}"
      echo "============================================"

      # Wrap per-file processing so one failure does not kill the batch
      if (
        set -euo pipefail

        # ---- Step 1: rsIDify ----
        temp_rsid="./temp_${trait}_${analysis_type}.rsid.tmp"

        python3 /scripts/rsidify_sqlite.py \
          "${local_input}" \
          --database "~{dbsnp_db}" \
          --chrom   "~{chrom_col}" \
          --pos     "~{pos_col}" \
          --ref     "~{ref_col}" \
          --alt     "~{alt_col}" \
          --output  "${temp_rsid}" 2>&1 | tee -a "${log_file}"

        total_variants=$(tail -n +2 "${temp_rsid}" | wc -l || echo 0)

        # ---- Step 2: MAC/MAF filter → intermediary (all columns + rsid) ----
        awk -F'\t' \
          -v sample_size="${sample_size}" \
          -v mac_threshold="~{mac_threshold}" \
          -v maf_threshold="~{maf_threshold}" \
          -v af_col_name="~{af_col}" \
        '
        BEGIN {
          getline; print $0
          for (i = 1; i <= NF; i++) if ($i == af_col_name) af_idx = i
          if (!af_idx) { print "Error: " af_col_name " column not found" > "/dev/stderr"; exit 1 }
        }
        {
          af  = $af_idx
          maf = (af > 0.5) ? 1 - af : af
          if (maf < maf_threshold) next
          if (maf * sample_size >= mac_threshold) print $0
        }' "${temp_rsid}" | gzip > \
          "outputs/intermediary/${trait}_${analysis_type}_mac~{mac_threshold}_maf~{maf_threshold}_intermediary.gz"

        # ---- Step 3: LDSC premunge (SNP A1 A2 BETA P) + multi-rsID handling ----
        awk -F'\t' \
          -v sample_size="${sample_size}" \
          -v mac_threshold="~{mac_threshold}" \
          -v maf_threshold="~{maf_threshold}" \
          -v af_col_name="~{af_col}" \
          -v beta_col_name="~{beta_col}" \
          -v pval_col_name="~{pval_col}" \
          -v alt_col_name="~{alt_col}" \
          -v ref_col_name="~{ref_col}" \
        '
        BEGIN {
          getline
          print "SNP", "A1", "A2", "BETA", "P"
          for (i = 1; i <= NF; i++) {
            if ($i == "rsid")          rsid_idx = i
            if ($i == alt_col_name)    alt_idx  = i
            if ($i == ref_col_name)    ref_idx  = i
            if ($i == beta_col_name)   beta_idx = i
            if ($i == pval_col_name)   pval_idx = i
            if ($i == af_col_name)     af_idx   = i
          }
          if (!rsid_idx || !alt_idx || !ref_idx || !beta_idx || !pval_idx || !af_idx) {
            print "Error: Required columns missing after rsIDify" > "/dev/stderr"
            exit 1
          }
        }
        {
          if ($rsid_idx == "" || $rsid_idx == "NA") next
          if ($af_idx  == "" || $af_idx  == "NA") next
          af  = $af_idx
          maf = (af > 0.5) ? 1 - af : af
          if (maf < maf_threshold) next
          if (maf * sample_size >= mac_threshold)
            print $rsid_idx, $alt_idx, $ref_idx, $beta_idx, $pval_idx
        }' "${temp_rsid}" > "./temp_${trait}_${analysis_type}.mac.tmp"

        filtered_variants=$(tail -n +2 "./temp_${trait}_${analysis_type}.mac.tmp" | wc -l || echo 0)

        # Explode comma-separated rsIDs into separate rows and compress
        awk 'BEGIN{FS=OFS="\t"} {n=split($1,a,","); for(i=1;i<=n;i++) print a[i],$0}' \
          "./temp_${trait}_${analysis_type}.mac.tmp" | cut -f1,3- | gzip > \
          "outputs/premunged/${trait}_${analysis_type}_mac~{mac_threshold}_maf~{maf_threshold}_premunged.gz"

        # Clean up temp files to reclaim disk
        rm -f "${temp_rsid}" "./temp_${trait}_${analysis_type}.mac.tmp"

        end_epoch=$(date +%s)
        duration=$((end_epoch - start_epoch))

        {
          echo "trait=${trait}"
          echo "analysis_type=${analysis_type}"
          echo "sample_size=${sample_size}"
          echo "mac_threshold=~{mac_threshold}"
          echo "maf_threshold=~{maf_threshold}"
          echo "total_variants=${total_variants}"
          echo "filtered_variants=${filtered_variants}"
          echo "duration_seconds=${duration}"
          echo "status=SUCCESS"
          echo "completed=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        } >> "${log_file}"

        echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] Done: ${trait} — ${filtered_variants}/${total_variants} passed (${duration}s)"

      ); then
        : # success — nothing extra to do
      else
        ((batch_failed++)) || true
        end_epoch=$(date +%s)
        duration=$((end_epoch - start_epoch))
        {
          echo "trait=${trait}"
          echo "analysis_type=${analysis_type}"
          echo "sample_size=${sample_size}"
          echo "mac_threshold=~{mac_threshold}"
          echo "maf_threshold=~{maf_threshold}"
          echo "total_variants=0"
          echo "filtered_variants=0"
          echo "duration_seconds=${duration}"
          echo "status=FAILED"
          echo "completed=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        } >> "${log_file}"
        echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] FAILED: ${trait} (${analysis_type})"
        # Clean up any partial temp files
        rm -f "./temp_${trait}_${analysis_type}"* 2>/dev/null || true
      fi

    done < ~{batch_manifest}

    echo ""
    echo "Batch processing complete. Failures in this batch: ${batch_failed}"

    if [[ ${batch_failed} -gt 0 ]]; then
      echo "WARNING: ${batch_failed} file(s) failed — check per-file logs." >&2
    fi
  >>>

  output {
    Array[File] premunged_files    = glob("outputs/premunged/*.gz")
    Array[File] intermediary_files = glob("outputs/intermediary/*.gz")
    Array[File] log_files          = glob("outputs/logs/*.log")
  }

  runtime {
    docker: docker
    cpu: 4
    memory: "16 GB"
    disks: "local-disk " + disk_size + " SSD"
    zones: "europe-west1-b europe-west1-c europe-west1-d"
    preemptible: 2
    noAddress: true
  }
}


# =============================================================================
# Task: gather_summary
# Parses per-file key=value log files into a single CSV summary.
# =============================================================================

task gather_summary {

  input {
    Array[File] log_files
    String      docker
  }

  command <<<
    set -uo pipefail

    header="trait,analysis_type,sample_size,mac_threshold,maf_threshold,total_variants,filtered_variants,duration_seconds,status,completed"
    echo "${header}" > summary.csv

    for log in ~{sep=" " log_files}; do
      trait="" ; analysis_type="" ; sample_size=""
      mac_threshold="" ; maf_threshold=""
      total_variants="" ; filtered_variants=""
      duration_seconds="" ; status="" ; completed=""

      while IFS='=' read -r key value; do
        case "${key}" in
          trait)              trait="${value}" ;;
          analysis_type)      analysis_type="${value}" ;;
          sample_size)        sample_size="${value}" ;;
          mac_threshold)      mac_threshold="${value}" ;;
          maf_threshold)      maf_threshold="${value}" ;;
          total_variants)     total_variants="${value}" ;;
          filtered_variants)  filtered_variants="${value}" ;;
          duration_seconds)   duration_seconds="${value}" ;;
          status)             status="${value}" ;;
          completed)          completed="${value}" ;;
        esac
      done < "${log}"

      echo "${trait},${analysis_type},${sample_size},${mac_threshold},${maf_threshold},${total_variants},${filtered_variants},${duration_seconds},${status},${completed}" >> summary.csv
    done

    total=$(( $(wc -l < summary.csv) - 1 ))
    success=$(grep -c ",SUCCESS," summary.csv || true)
    failed=$(grep -c ",FAILED," summary.csv || true)
    echo ""
    echo "===== Pipeline Summary ====="
    echo "Total files processed: ${total}"
    echo "Succeeded:             ${success}"
    echo "Failed:                ${failed}"
    echo "============================"
  >>>

  output {
    File summary = "summary.csv"
  }

  runtime {
    docker: docker
    cpu: 1
    memory: "2 GB"
    disks: "local-disk 10 HDD"
    zones: "europe-west1-b europe-west1-c europe-west1-d"
    preemptible: 2
    noAddress: true
  }
}
