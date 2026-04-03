#!/usr/bin/env python3
"""
Copy mac_filter_rsidify Cromwell outputs into a flat chunk layout on GCS.

Designed for runs that have **already finished**. Uses one of:

  * Cromwell REST ``/outputs`` JSON (via curl + SOCKS, same pattern as
    CromwellInteract), or a **saved** copy of that JSON.
  * **GCS workflow root** scan: lists ``call-rsidify_and_filter_batch/`` and
    picks the correct ``attempt-*`` per shard (from ``*_intermediary.gz``), or
    ``shard-N/glob-*`` paths when Cromwell omitted ``attempt-*`` in the URI —
    **no SSH tunnel** if you can read the execution bucket.

Destination layout (under ``--dest``):

  * ``intermediary/``     — WDL ``intermediary_outputs`` (*_intermediary.gz)
  * ``logs/``             — WDL ``per_file_logs`` (per-trait pipeline logs)
  * ``mac_filtered_output/`` — WDL ``premunged_outputs`` (*_premunged.gz)

Requires ``gsutil`` on PATH for copies.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

KEY_INTERMEDIARY = "mac_filter_rsidify.intermediary_outputs"
KEY_PREMUNGED = "mac_filter_rsidify.premunged_outputs"
KEY_LOGS = "mac_filter_rsidify.per_file_logs"
KEY_SUMMARY = "mac_filter_rsidify.summary_csv"

# Cromwell may delocalise under shard-N/attempt-M/glob-*/ (retries) or
# shard-N/glob-*/ (final copy without attempt in the path).
SHARD_ATTEMPT_GLOB = re.compile(
    r"(?P<prefix>.*)/call-rsidify_and_filter_batch/shard-(?P<shard>\d+)"
    r"/attempt-(?P<attempt>\d+)/glob-[^/]+/(?P<basename>[^/]+)$"
)
SHARD_DIRECT_GLOB = re.compile(
    r"(?P<prefix>.*)/call-rsidify_and_filter_batch/shard-(?P<shard>\d+)"
    r"/glob-[^/]+/(?P<basename>[^/]+)$"
)


def flatten_file_list(value: Any) -> list[str]:
    """Flatten Cromwell File / Array[File] JSON into a list of gs:// URIs."""
    out: list[str] = []
    if value is None:
        return out
    if isinstance(value, str):
        if value.startswith("gs://"):
            out.append(value)
        return out
    if isinstance(value, list):
        for item in value:
            out.extend(flatten_file_list(item))
        return out
    return out


def normalise_dest_base(uri: str) -> str:
    uri = uri.rstrip("/")
    if not uri.startswith("gs://"):
        raise ValueError(f"Destination must be a gs:// URI, got: {uri!r}")
    return uri


def load_outputs_payload(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if "outputs" in raw and isinstance(raw["outputs"], dict):
        return raw["outputs"]
    if isinstance(raw, dict) and any(k.startswith("mac_filter_rsidify.") for k in raw):
        return raw
    raise ValueError(
        "JSON must be either a full Cromwell /outputs response "
        "({'outputs': {...}}) or the inner outputs object."
    )


def fetch_outputs_via_curl(
    workflow_id: str,
    socks_port: int,
    http_port: int,
    timeout: int,
) -> dict[str, Any]:
    """Mirror CromwellInteract.get_outputs: curl + SOCKS5 to localhost."""
    url = f"http://localhost:{http_port}/api/workflows/v1/{workflow_id}/outputs"
    cmd = (
        f'curl -sS -X GET {shlex.quote(url)} -H "accept: application/json" '
        f"--socks5 localhost:{socks_port}"
    )
    pr = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if pr.returncode != 0:
        raise RuntimeError(f"curl failed (exit {pr.returncode}): {pr.stderr.strip()}")
    data = json.loads(pr.stdout)
    if data.get("status") == "fail":
        raise RuntimeError(f"Cromwell API error: {data.get('message', data)}")
    if "outputs" not in data:
        snippet = repr(data)[:500]
        raise RuntimeError(f"Unexpected response (no 'outputs' key): {snippet}")
    return data["outputs"]


def classify_from_outputs(outputs: dict[str, Any]) -> dict[str, list[str]]:
    return {
        "intermediary": flatten_file_list(outputs.get(KEY_INTERMEDIARY)),
        "mac_filtered_output": flatten_file_list(outputs.get(KEY_PREMUNGED)),
        "logs": flatten_file_list(outputs.get(KEY_LOGS)),
        "summary": flatten_file_list(outputs.get(KEY_SUMMARY)),
    }


def _is_pipeline_log(basename: str) -> bool:
    if not basename.endswith(".log"):
        return False
    if basename.startswith("rsidify_and_filter_batch"):
        return False
    if basename.startswith("stderr-job") or basename.startswith("stdout-job"):
        return False
    if basename == "monitoring.log":
        return False
    return True


def discover_from_gcs_workflow_root(workflow_root: str) -> dict[str, list[str]]:
    """
    List all objects under call-rsidify_and_filter_batch. Per shard, either:

    * use the highest ``attempt-*`` that contains ``*_intermediary.gz``, or
    * if outputs only exist under ``shard-N/glob-*`` (no attempt in path),
      use those paths.
    """
    workflow_root = workflow_root.rstrip("/")
    batch_prefix = f"{workflow_root}/call-rsidify_and_filter_batch/"
    cmd = ["gsutil", "ls", "-r", batch_prefix]
    pr = subprocess.run(cmd, capture_output=True, text=True, timeout=600, check=False)
    if pr.returncode != 0:
        raise RuntimeError(f"gsutil ls failed: {pr.stderr.strip()}")

    lines = [ln.strip() for ln in pr.stdout.splitlines() if ln.strip()]

    max_attempt: dict[int, int] = {}
    for line in lines:
        m = SHARD_ATTEMPT_GLOB.match(line)
        if not m or not m.group("basename").endswith("_intermediary.gz"):
            continue
        shard = int(m.group("shard"))
        att = int(m.group("attempt"))
        max_attempt[shard] = max(max_attempt.get(shard, 0), att)

    use_direct_shard: set[int] = set()
    for line in lines:
        md = SHARD_DIRECT_GLOB.match(line)
        if md and line.endswith("_intermediary.gz"):
            sh = int(md.group("shard"))
            if sh not in max_attempt:
                use_direct_shard.add(sh)

    intermediary: list[str] = []
    premunged: list[str] = []
    logs: list[str] = []

    for line in lines:
        m_att = SHARD_ATTEMPT_GLOB.match(line)
        m_dir = None if m_att else SHARD_DIRECT_GLOB.match(line)
        if not m_att and not m_dir:
            continue

        if m_att:
            shard = int(m_att.group("shard"))
            att = int(m_att.group("attempt"))
            if shard in use_direct_shard:
                continue
            want = max_attempt.get(shard)
            if want is None or att != want:
                continue
            base = m_att.group("basename")
        else:
            assert m_dir is not None
            shard = int(m_dir.group("shard"))
            if shard not in use_direct_shard:
                continue
            base = m_dir.group("basename")

        if base.endswith("_intermediary.gz"):
            intermediary.append(line)
        elif base.endswith("_premunged.gz"):
            premunged.append(line)
        elif _is_pipeline_log(base):
            logs.append(line)

    summary: list[str] = []
    summary_uri = f"{workflow_root}/call-gather_summary/summary.csv"
    check = subprocess.run(
        ["gsutil", "ls", summary_uri],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if check.returncode == 0 and summary_uri in check.stdout.strip().splitlines():
        summary.append(summary_uri)

    return {
        "intermediary": sorted(set(intermediary)),
        "mac_filtered_output": sorted(set(premunged)),
        "logs": sorted(set(logs)),
        "summary": summary,
    }


def run_gsutil_cp(src: str, dst: str) -> tuple[str, bool, str]:
    pr = subprocess.run(
        ["gsutil", "-q", "cp", src, dst],
        capture_output=True,
        text=True,
        timeout=7200,
        check=False,
    )
    ok = pr.returncode == 0
    msg = (pr.stderr or pr.stdout or "").strip()
    return src, ok, msg


def copy_category(
    uris: list[str],
    dest_dir: str,
    dry_run: bool,
    workers: int,
) -> tuple[int, int]:
    """Copy URIs to dest_dir/ preserving basenames. Returns (ok_count, fail_count)."""
    if not uris:
        return 0, 0
    dest_dir = dest_dir.rstrip("/") + "/"
    if dry_run:
        print(f"  [dry-run] would copy {len(uris)} objects -> {dest_dir}")
        return len(uris), 0

    ok_n = 0
    fail_n = 0
    tasks = [(u, dest_dir + Path(urlparse(u).path).name) for u in uris]

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = {ex.submit(run_gsutil_cp, s, d): (s, d) for s, d in tasks}
        for fut in as_completed(futs):
            src, ok, msg = fut.result()
            if ok:
                ok_n += 1
            else:
                fail_n += 1
                print(f"ERROR cp {src}: {msg}", file=sys.stderr)

    return ok_n, fail_n


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Harvest mac_filter_rsidify outputs from Cromwell into gs:// chunk folders."
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--outputs-json",
        type=Path,
        help="Path to saved Cromwell GET .../outputs JSON (full response or inner 'outputs' object).",
    )
    src.add_argument(
        "--workflow-id",
        type=str,
        help="Fetch outputs from Cromwell via curl (requires SSH tunnel / SOCKS).",
    )
    src.add_argument(
        "--gcs-workflow-root",
        type=str,
        metavar="GS_URI",
        help="Scan this workflow root on GCS (e.g. gs://cromwell-fg-3/mac_filter_rsidify/<uuid>/). "
        "No Cromwell tunnel; uses highest attempt-* per shard.",
    )

    parser.add_argument(
        "--dest",
        type=str,
        required=True,
        help="Base gs:// URI (e.g. gs://bucket/fg3_aggregate_pQTLs/02.chunk_0501_1000).",
    )
    parser.add_argument(
        "--intermediary-subdir",
        default="intermediary",
        help="Subdirectory under --dest for intermediary .gz (default: intermediary).",
    )
    parser.add_argument(
        "--logs-subdir",
        default="logs",
        help="Subdirectory under --dest for per-trait logs (default: logs).",
    )
    parser.add_argument(
        "--mac-filtered-subdir",
        default="mac_filtered_output",
        help="Subdirectory under --dest for premunged .gz (default: mac_filtered_output).",
    )
    parser.add_argument(
        "--socks-port",
        type=int,
        default=5000,
        help="SOCKS port for curl (default: 5000, CromwellInteract default).",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=80,
        help="Cromwell HTTP port on remote (default: 80, CromwellInteract default).",
    )
    parser.add_argument(
        "--curl-timeout",
        type=int,
        default=120,
        help="Seconds to wait for Cromwell /outputs (default: 120).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Parallel gsutil cp workers (default: 32).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print counts only; do not copy.",
    )
    parser.add_argument(
        "--copy-summary",
        action="store_true",
        help="Also copy workflow summary.csv into the logs subfolder as summary.csv.",
    )

    args = parser.parse_args()
    dest_base = normalise_dest_base(args.dest)

    if args.outputs_json is not None:
        classified = classify_from_outputs(load_outputs_payload(args.outputs_json))
    elif args.workflow_id is not None:
        outputs = fetch_outputs_via_curl(
            args.workflow_id,
            args.socks_port,
            args.http_port,
            args.curl_timeout,
        )
        classified = classify_from_outputs(outputs)
    else:
        classified = discover_from_gcs_workflow_root(args.gcs_workflow_root)

    subdirs = {
        "intermediary": f"{dest_base}/{args.intermediary_subdir}",
        "mac_filtered_output": f"{dest_base}/{args.mac_filtered_subdir}",
        "logs": f"{dest_base}/{args.logs_subdir}",
    }

    print("Harvest plan:")
    for key, label in (
        ("intermediary", "intermediary .gz"),
        ("mac_filtered_output", "premunged .gz (mac_filtered_output/)"),
        ("logs", "per-trait logs"),
    ):
        n = len(classified[key])
        print(f"  {label}: n={n}")

    if args.copy_summary and classified.get("summary"):
        print(f"  summary.csv: {classified['summary'][0]}")

    fails = 0
    for key in ("intermediary", "mac_filtered_output", "logs"):
        ok, bad = copy_category(
            classified[key],
            subdirs[key],
            dry_run=args.dry_run,
            workers=args.workers,
        )
        print(f"  copied {key}: {ok} ok, {bad} failed")
        fails += bad

    if args.copy_summary and classified.get("summary") and not args.dry_run:
        src = classified["summary"][0]
        dst = f"{subdirs['logs']}/summary.csv"
        _, ok, msg = run_gsutil_cp(src, dst)
        if not ok:
            print(f"ERROR summary cp: {msg}", file=sys.stderr)
            fails += 1
        else:
            print("  summary.csv -> logs/summary.csv")

    if fails:
        sys.exit(1)


if __name__ == "__main__":
    main()
