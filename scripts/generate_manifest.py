#!/usr/bin/env python3
"""
Generate a manifest TSV for the mac_filter_rsidify WDL pipeline.

Discovers .gz summary-statistics files in a GCS or local directory and
writes a headerless, tab-delimited manifest with columns:

    trait  analysis_type  gs://path_or_local_path  sample_size

Usage examples
--------------
Uniform sample size for all files::

    python3 generate_manifest.py \\
        --sumstats-dir gs://bucket/path/to/sumstats \\
        --sample-size 4144 \\
        --analysis-type pqtl \\
        --output manifest.tsv

Per-trait sample sizes from a TSV mapping file::

    python3 generate_manifest.py \\
        --sumstats-dir gs://bucket/path/to/sumstats \\
        --sample-size-map sample_sizes.tsv \\
        --analysis-type pqtl \\
        --output manifest.tsv

The mapping file (``--sample-size-map``) is a headerless, tab-delimited
file with two columns: ``trait_name`` and ``sample_size``.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys


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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a manifest TSV for mac_filter_rsidify"
    )
    parser.add_argument(
        "--sumstats-dir",
        required=True,
        help="Directory containing .gz sumstats (local or gs://)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        help="Uniform sample size applied to every file",
    )
    parser.add_argument(
        "--sample-size-map",
        help="Headerless TSV mapping trait_name → sample_size (overrides --sample-size)",
    )
    parser.add_argument(
        "--analysis-type",
        default="pqtl",
        help="Analysis-type label for all entries (default: pqtl)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="manifest.tsv",
        help="Output manifest path (default: manifest.tsv)",
    )

    args = parser.parse_args()

    if args.sample_size is None and args.sample_size_map is None:
        parser.error("Provide either --sample-size or --sample-size-map")

    files = list_gz_files(args.sumstats_dir)
    if not files:
        print(f"Error: No .gz files found in {args.sumstats_dir}", file=sys.stderr)
        sys.exit(1)

    ss_map: dict[str, str] = {}
    if args.sample_size_map:
        ss_map = load_sample_size_map(args.sample_size_map)

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
            out.write(
                f"{trait}\t{args.analysis_type}\t{filepath}\t{sample_size}\n"
            )
            written += 1

    print(f"Manifest written: {args.output}")
    print(f"Total entries:    {written}")


if __name__ == "__main__":
    main()
