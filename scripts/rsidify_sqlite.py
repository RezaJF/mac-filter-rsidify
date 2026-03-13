#!/usr/bin/env python3
"""
RSIDify — annotate GWAS summary statistics with rsIDs via SQLite dbSNP lookup.

Reads a (possibly gzipped) summary statistics file, queries a local dbSNP
SQLite database for each variant's rsID based on (chrom, pos, ref, alt),
and writes an augmented file with an ``rsid`` column prepended.

The database must contain a table ``variants`` with columns:
    chrom  TEXT
    pos    INTEGER
    ref    TEXT
    alt    TEXT
    rsid   TEXT

with an index on (chrom, pos, ref, alt).

This script is designed to run inside a Docker container on Cromwell/Refinery
with no hardcoded paths — all file locations are passed as arguments.
"""

import sqlite3
import gzip
import sys
import argparse
import time


def process_sumstats_with_sqlite(
    input_file: str,
    db_file: str,
    chrom_col: str,
    pos_col: str,
    ref_col: str,
    alt_col: str,
    sep: str = "\t",
    output_file: str | None = None,
) -> None:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    is_gzipped = input_file.endswith(".gz")
    infile = gzip.open(input_file, "rt") if is_gzipped else open(input_file, "r")
    outfile = open(output_file, "w") if output_file else sys.stdout

    try:
        header = infile.readline().strip()
        cols = header.split(sep)

        try:
            chrom_idx = cols.index(chrom_col)
            pos_idx = cols.index(pos_col)
            ref_idx = cols.index(ref_col)
            alt_idx = cols.index(alt_col)
        except ValueError as exc:
            print(f"Error: Column not found — {exc}", file=sys.stderr)
            sys.exit(1)

        outfile.write("rsid" + sep + header + "\n")

        line_count = 0
        found_count = 0
        not_found_count = 0
        max_idx = max(chrom_idx, pos_idx, ref_idx, alt_idx)
        t0 = time.time()

        for line in infile:
            line_count += 1

            if line_count % 500_000 == 0:
                elapsed = time.time() - t0
                rate = line_count / elapsed if elapsed else 0
                print(
                    f"  Processed {line_count:,} variants, "
                    f"{found_count:,} with rsIDs "
                    f"({rate:,.0f} variants/s)",
                    file=sys.stderr,
                )

            parts = line.strip().split(sep)
            if len(parts) <= max_idx:
                outfile.write(sep + line)
                continue

            chrom = parts[chrom_idx].replace("chr", "").replace("#", "")
            try:
                pos = int(parts[pos_idx])
            except ValueError:
                outfile.write(sep + line)
                continue

            ref = parts[ref_idx].upper()
            alt = parts[alt_idx].upper()

            cursor.execute(
                "SELECT rsid FROM variants "
                "WHERE chrom=? AND pos=? AND ref=? AND alt=? LIMIT 1",
                (chrom, pos, ref, alt),
            )
            result = cursor.fetchone()

            if result:
                rsid = result[0]
                found_count += 1
            else:
                rsid = ""
                not_found_count += 1

            outfile.write(rsid + sep + line)

        elapsed = time.time() - t0
        print(f"\nProcessing completed in {elapsed:.1f}s:", file=sys.stderr)
        print(f"  Total variants:   {line_count:,}", file=sys.stderr)
        if line_count > 0:
            print(
                f"  With rsIDs:       {found_count:,} "
                f"({found_count * 100 / line_count:.1f}%)",
                file=sys.stderr,
            )
            print(
                f"  Without rsIDs:    {not_found_count:,} "
                f"({not_found_count * 100 / line_count:.1f}%)",
                file=sys.stderr,
            )

    finally:
        infile.close()
        if output_file:
            outfile.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Add rsIDs to summary statistics using a SQLite dbSNP database"
    )
    parser.add_argument("input", help="Input summary statistics file (.gz or plain text)")
    parser.add_argument(
        "--database", "-d", required=True, help="Path to SQLite dbSNP database"
    )
    parser.add_argument("--output", "-o", help="Output file (default: stdout)")
    parser.add_argument("--chrom", "-c", required=True, help="Chromosome column name")
    parser.add_argument("--pos", "-p", required=True, help="Position column name")
    parser.add_argument("--ref", "-r", required=True, help="Reference allele column name")
    parser.add_argument("--alt", "-a", required=True, help="Alternative allele column name")
    parser.add_argument("--sep", "-s", default="\t", help="Column separator (default: tab)")

    args = parser.parse_args()

    try:
        conn = sqlite3.connect(args.database)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM variants LIMIT 1")
        count = cursor.fetchone()[0]
        print(f"Using dbSNP database with {count:,} variants", file=sys.stderr)
        conn.close()
    except Exception as exc:
        print(f"Error: Cannot access database — {exc}", file=sys.stderr)
        sys.exit(1)

    process_sumstats_with_sqlite(
        args.input,
        args.database,
        args.chrom,
        args.pos,
        args.ref,
        args.alt,
        args.sep,
        args.output,
    )


if __name__ == "__main__":
    main()
