#!/usr/bin/env python3
"""P12 — Benchmark Analysis Script

Reads:
  - target/criterion/<group>/<param>/estimates.json  (Criterion output)
  - reports/csv/*.csv                                (pipeline metrics dumps)

Prints a human-readable summary table to stdout and writes a machine-readable
summary to reports/csv/benchmark_summary.csv.

Usage:
    python scripts/analyse_bench.py [--criterion-dir target/criterion]
                                    [--reports-dir   reports/csv]
                                    [--out           reports/csv/benchmark_summary.csv]
"""

import argparse
import csv
import json
import sys
from pathlib import Path


# ── Criterion helpers ─────────────────────────────────────────────────────────

def load_criterion_estimates(criterion_dir: Path) -> list[dict]:
    """Walk criterion_dir and collect one row per (group, param) pair."""
    rows = []
    for estimates_path in sorted(criterion_dir.rglob("estimates.json")):
        # Path structure: <criterion_dir>/<group>/<param>/estimates.json
        parts = estimates_path.relative_to(criterion_dir).parts
        if len(parts) < 3:
            continue
        group = parts[0]
        param = parts[1]

        try:
            with estimates_path.open() as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            print(f"  [warn] could not read {estimates_path}: {exc}", file=sys.stderr)
            continue

        mean_ns = data.get("mean", {}).get("point_estimate", float("nan"))
        std_ns  = data.get("std_dev", {}).get("point_estimate", float("nan"))
        rows.append({
            "group":   group,
            "param":   param,
            "mean_ns": mean_ns,
            "std_ns":  std_ns,
        })
    return rows


def format_ns(ns: float) -> str:
    """Format nanoseconds as a human-readable string."""
    if ns != ns:  # NaN
        return "N/A"
    if ns < 1_000:
        return f"{ns:.1f} ns"
    if ns < 1_000_000:
        return f"{ns/1_000:.2f} µs"
    if ns < 1_000_000_000:
        return f"{ns/1_000_000:.2f} ms"
    return f"{ns/1_000_000_000:.3f} s"


def print_criterion_table(rows: list[dict]) -> None:
    if not rows:
        print("  (no Criterion data found — run `cargo bench -p rts-bench` first)")
        return

    # Group by benchmark group name
    groups: dict[str, list[dict]] = {}
    for row in rows:
        groups.setdefault(row["group"], []).append(row)

    for group_name, group_rows in groups.items():
        print(f"\n  [{group_name}]")
        print(f"  {'Param':<20}  {'Mean':>12}  {'Std Dev':>12}")
        print(f"  {'-'*20}  {'-'*12}  {'-'*12}")
        for row in sorted(group_rows, key=lambda r: r["param"]):
            print(
                f"  {row['param']:<20}  "
                f"{format_ns(row['mean_ns']):>12}  "
                f"{format_ns(row['std_ns']):>12}"
            )


# ── Pipeline CSV helpers ──────────────────────────────────────────────────────

def load_pipeline_csvs(reports_dir: Path) -> list[dict]:
    """Load all *.csv files from reports_dir (pipeline metrics dumps)."""
    rows = []
    for csv_path in sorted(reports_dir.glob("*.csv")):
        if csv_path.name == "benchmark_summary.csv":
            continue
        try:
            with csv_path.open(newline="") as f:
                reader = csv.DictReader(f)
                for record in reader:
                    record["source_file"] = csv_path.name
                    rows.append(record)
        except OSError as exc:
            print(f"  [warn] could not read {csv_path}: {exc}", file=sys.stderr)
    return rows


def print_pipeline_table(rows: list[dict]) -> None:
    if not rows:
        print(
            "  (no pipeline CSV data found — run with --metrics-path to generate)"
        )
        return

    header = ["source_file", "metric", "p50", "p90", "p99", "p99.9", "sample_count"]
    col_w = [max(len(h), max((len(str(r.get(h, ""))) for r in rows), default=0))
             for h in header]

    def fmt_row(values):
        return "  " + "  ".join(str(v).ljust(w) for v, w in zip(values, col_w))

    print()
    print(fmt_row(header))
    print("  " + "  ".join("-" * w for w in col_w))
    for row in rows:
        print(fmt_row([row.get(h, "") for h in header]))


# ── Summary CSV writer ────────────────────────────────────────────────────────

def write_summary_csv(
    criterion_rows: list[dict],
    pipeline_rows: list[dict],
    out_path: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["section", "group_or_file", "param_or_metric",
                         "mean_ns_or_p50", "std_ns_or_p90", "p99", "p99.9",
                         "sample_count"])
        for r in criterion_rows:
            writer.writerow([
                "criterion", r["group"], r["param"],
                r["mean_ns"], r["std_ns"], "", "", "",
            ])
        for r in pipeline_rows:
            writer.writerow([
                "pipeline",
                r.get("source_file", ""),
                r.get("metric", ""),
                r.get("p50", ""),
                r.get("p90", ""),
                r.get("p99", ""),
                r.get("p99.9", ""),
                r.get("sample_count", ""),
            ])
    print(f"\n  Summary written to: {out_path}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--criterion-dir", default="target/criterion",
                        help="Root of Criterion output tree (default: target/criterion)")
    parser.add_argument("--reports-dir", default="reports/csv",
                        help="Directory holding pipeline *.csv dumps (default: reports/csv)")
    parser.add_argument("--out", default="reports/csv/benchmark_summary.csv",
                        help="Path for the consolidated summary CSV")
    args = parser.parse_args()

    criterion_dir = Path(args.criterion_dir)
    reports_dir   = Path(args.reports_dir)
    out_path      = Path(args.out)

    print("=== RTS2601 Benchmark Analysis ===")

    print("\n── Criterion results ────────────────────────────────")
    criterion_rows = load_criterion_estimates(criterion_dir)
    print_criterion_table(criterion_rows)

    print("\n── Pipeline latency CSVs ────────────────────────────")
    pipeline_rows = load_pipeline_csvs(reports_dir)
    print_pipeline_table(pipeline_rows)

    write_summary_csv(criterion_rows, pipeline_rows, out_path)


if __name__ == "__main__":
    main()
