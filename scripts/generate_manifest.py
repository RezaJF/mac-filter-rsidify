#!/usr/bin/env python3
"""
Generate a manifest TSV for the mac_filter_rsidify WDL pipeline.

Discovers .gz summary-statistics files in a GCS or local directory and
writes a headerless, tab-delimited manifest with columns:

    trait  analysis_type  gs://path_or_local_path  sample_size

After writing the TSV the script:

  1. Uploads (or copies) the manifest to ``--sumstats-dir`` so it lives
     alongside the input sumstats.
  2. Writes a ready-to-submit Cromwell inputs JSON whose name matches the
     manifest stem (e.g. ``04.chunk_1501_2000.tsv`` → ``04.chunk_1501_2000.json``)
     in the same local directory as the TSV.

Usage examples
--------------
Uniform sample size, all defaults::

    python3 generate_manifest.py \\
        --sumstats-dir gs://r13-data/fg3_aggregate_pQTLs/04.chunk_1501_2000/ \\
        --sample-size 4144 \\
        --analysis-type pqtl \\
        --output wdl/04.chunk_1501_2000.tsv

Per-trait sample sizes from a mapping file::

    python3 generate_manifest.py \\
        --sumstats-dir gs://bucket/path/to/sumstats \\
        --sample-size-map sample_sizes.tsv \\
        --analysis-type pqtl \\
        --output wdl/04.chunk_1501_2000.tsv

The mapping file (``--sample-size-map``) is a headerless, tab-delimited
file with two columns: ``trait_name`` and ``sample_size``.

Skip side-effects::

    # No GCS upload, no JSON
    python3 generate_manifest.py ... --no-upload --no-json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys


# ---------------------------------------------------------------------------
# Constants – production defaults used in all real pQTL chunk runs
# ---------------------------------------------------------------------------

_DEFAULT_DBSNP   = "gs://r13-data/reza/dbsnp_b155/dbsnp_b155_hg38.db"
_DEFAULT_DOCKER  = (
    "europe-west1-docker.pkg.dev/finngen-refinery-dev/"
    "fg-refinery-registry/mac-filter-rsidify:v1"
)
_DEFAULT_MAC     = 30
_DEFAULT_MAF     = 0.0001
_DEFAULT_BATCH   = 1
_DEFAULT_CHROM   = "#chrom"
_DEFAULT_POS     = "pos"
_DEFAULT_REF     = "ref"
_DEFAULT_ALT     = "alt"
_DEFAULT_AF      = "af_alt"
_DEFAULT_BETA    = "beta"
_DEFAULT_PVAL    = "pval"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def list_gz_files(directory: str) -> list[str]:
    """Return sorted list of .gz file paths (excluding .tbi) in *directory*."""
    if directory.startswith("gs://"):
        result = subprocess.run(
            ["gsutil", "ls", f"{directory.rstrip('/')}/*.gz"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"Error listing files: {result.stderr}", file=sys.stderr)
            sys.exit(1)
        paths = [
            p.strip()
            for p in result.stdout.strip().split("\n")
            if p.strip() and not p.strip().endswith(".tbi")
        ]
    else:
        directory = os.path.abspath(directory)
        paths = [
            os.path.join(directory, f)
            for f in sorted(os.listdir(directory))
            if f.endswith(".gz") and not f.endswith(".tbi")
        ]
    return sorted(paths)


def extract_trait_name(filepath: str) -> str:
    """Derive a trait name from the basename (strip .gz extension)."""
    return os.path.basename(filepath).removesuffix(".gz")


def load_sample_size_map(path: str) -> dict[str, str]:
    """Load a headerless TSV of trait_name → sample_size."""
    mapping: dict[str, str] = {}
    with open(path) as fh:
        for line in fh:
            parts = line.strip().split("\t")
            if len(parts) >= 2:
                mapping[parts[0]] = parts[1]
    return mapping


def upload_manifest(local_path: str, sumstats_dir: str) -> str:
    """Copy the manifest to *sumstats_dir*; return the destination path."""
    basename = os.path.basename(local_path)
    if sumstats_dir.startswith("gs://"):
        dest = f"{sumstats_dir.rstrip('/')}/{basename}"
        result = subprocess.run(
            ["gsutil", "cp", local_path, dest],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(
                f"Error uploading manifest to GCS: {result.stderr}",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"Manifest uploaded: {dest}")
    else:
        dest = os.path.join(os.path.abspath(sumstats_dir), basename)
        if os.path.abspath(local_path) != dest:
            shutil.copy2(local_path, dest)
        print(f"Manifest copied:   {dest}")
    return dest


def write_inputs_json(
    local_tsv_path: str,
    gcs_manifest_path: str,
    args: argparse.Namespace,
) -> str:
    """Write a Cromwell inputs JSON beside the TSV; return its path."""
    stem = os.path.splitext(local_tsv_path)[0]
    json_path = f"{stem}.json"

    payload = {
        "mac_filter_rsidify.sumstats_manifest": gcs_manifest_path,
        "mac_filter_rsidify.dbsnp_database":    args.dbsnp_database,

        "mac_filter_rsidify.mac_threshold": args.mac_threshold,
        "mac_filter_rsidify.maf_threshold": args.maf_threshold,
        "mac_filter_rsidify.batch_size":    args.batch_size,

        "mac_filter_rsidify.docker": args.docker,

        "mac_filter_rsidify.chrom_col": args.chrom_col,
        "mac_filter_rsidify.pos_col":   args.pos_col,
        "mac_filter_rsidify.ref_col":   args.ref_col,
        "mac_filter_rsidify.alt_col":   args.alt_col,
        "mac_filter_rsidify.af_col":    args.af_col,
        "mac_filter_rsidify.beta_col":  args.beta_col,
        "mac_filter_rsidify.pval_col":  args.pval_col,
    }

    with open(json_path, "w") as fh:
        json.dump(payload, fh, indent=4)
        fh.write("\n")

    print(f"Inputs JSON written: {json_path}")
    return json_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a manifest TSV for mac_filter_rsidify, upload it to "
            "--sumstats-dir, and write a matching Cromwell inputs JSON."
        )
    )

    # ---- manifest inputs ----
    parser.add_argument(
        "--sumstats-dir",
        required=True,
        help="Directory containing .gz sumstats (local or gs://).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        help="Uniform sample size applied to every file.",
    )
    parser.add_argument(
        "--sample-size-map",
        help=(
            "Headerless TSV mapping trait_name → sample_size "
            "(overrides --sample-size)."
        ),
    )
    parser.add_argument(
        "--analysis-type",
        default="pqtl",
        help="Analysis-type label for all entries (default: pqtl).",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="manifest.tsv",
        help="Local output manifest path (default: manifest.tsv).",
    )

    # ---- WDL pipeline parameters written into the JSON ----
    parser.add_argument(
        "--dbsnp-database",
        default=_DEFAULT_DBSNP,
        help=f"GCS path to dbSNP SQLite database (default: {_DEFAULT_DBSNP}).",
    )
    parser.add_argument(
        "--docker",
        default=_DEFAULT_DOCKER,
        help=f"Docker image URI (default: {_DEFAULT_DOCKER}).",
    )
    parser.add_argument(
        "--mac-threshold",
        type=int,
        default=_DEFAULT_MAC,
        help=f"Minimum Minor Allele Count (default: {_DEFAULT_MAC}).",
    )
    parser.add_argument(
        "--maf-threshold",
        type=float,
        default=_DEFAULT_MAF,
        help=f"Minimum Minor Allele Frequency (default: {_DEFAULT_MAF}).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_DEFAULT_BATCH,
        help=f"Files processed per VM (default: {_DEFAULT_BATCH}).",
    )
    parser.add_argument("--chrom-col", default=_DEFAULT_CHROM,
                        help=f"Chromosome column name (default: {_DEFAULT_CHROM}).")
    parser.add_argument("--pos-col",   default=_DEFAULT_POS,
                        help=f"Position column name (default: {_DEFAULT_POS}).")
    parser.add_argument("--ref-col",   default=_DEFAULT_REF,
                        help=f"Reference allele column name (default: {_DEFAULT_REF}).")
    parser.add_argument("--alt-col",   default=_DEFAULT_ALT,
                        help=f"Alternative allele column name (default: {_DEFAULT_ALT}).")
    parser.add_argument("--af-col",    default=_DEFAULT_AF,
                        help=f"Allele frequency column name (default: {_DEFAULT_AF}).")
    parser.add_argument("--beta-col",  default=_DEFAULT_BETA,
                        help=f"Effect size column name (default: {_DEFAULT_BETA}).")
    parser.add_argument("--pval-col",  default=_DEFAULT_PVAL,
                        help=f"P-value column name (default: {_DEFAULT_PVAL}).")

    # ---- side-effect opt-outs ----
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip uploading/copying the manifest to --sumstats-dir.",
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Skip writing the Cromwell inputs JSON.",
    )

    args = parser.parse_args()

    if args.sample_size is None and args.sample_size_map is None:
        parser.error("Provide either --sample-size or --sample-size-map")

    # ---- 1. Discover sumstats files ----
    files = list_gz_files(args.sumstats_dir)
    if not files:
        print(f"Error: No .gz files found in {args.sumstats_dir}", file=sys.stderr)
        sys.exit(1)

    # ---- 2. Optionally load per-trait sample-size map ----
    ss_map: dict[str, str] = {}
    if args.sample_size_map:
        ss_map = load_sample_size_map(args.sample_size_map)

    # ---- 3. Write local manifest TSV ----
    written = 0
    with open(args.output, "w") as out:
        for filepath in files:
            trait = extract_trait_name(filepath)
            sample_size = ss_map.get(trait, str(args.sample_size or ""))
            if not sample_size:
                print(
                    f"Warning: No sample size for trait '{trait}', skipping",
                    file=sys.stderr,
                )
                continue
            out.write(f"{trait}\t{args.analysis_type}\t{filepath}\t{sample_size}\n")
            written += 1

    print(f"Manifest written: {args.output}")
    print(f"Total entries:    {written}")

    # ---- 4. Upload / copy manifest to --sumstats-dir ----
    gcs_manifest_path = args.output   # fallback: if upload skipped, JSON points at local path
    if not args.no_upload:
        gcs_manifest_path = upload_manifest(args.output, args.sumstats_dir)

    # ---- 5. Write companion Cromwell inputs JSON ----
    if not args.no_json:
        write_inputs_json(args.output, gcs_manifest_path, args)


if __name__ == "__main__":
    main()
