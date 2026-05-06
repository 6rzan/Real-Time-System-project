#!/usr/bin/env python3
"""P12 — Latency Comparison Plotter

Reads pipeline metrics CSVs produced by --metrics-path and generates:
  1. reports/plots/latency_cdf.png      — empirical CDF of drift p-values
  2. reports/plots/latency_bar.png      — side-by-side bar chart (async vs threaded)

Requires: matplotlib, numpy  (pip install matplotlib numpy)

Usage:
    python scripts/plot_latency.py [--reports-dir reports/csv]
                                   [--out-dir     reports/plots]
                                   [--no-show]

If the pipeline CSVs are absent the script prints usage instructions and exits 0.
"""

import argparse
import csv
import sys
from pathlib import Path


def _require_matplotlib():
    try:
        import matplotlib  # noqa: F401
        import numpy       # noqa: F401
    except ImportError:
        print(
            "ERROR: matplotlib and numpy are required.\n"
            "  pip install matplotlib numpy",
            file=sys.stderr,
        )
        sys.exit(1)


def load_csv_metrics(reports_dir: Path) -> dict[str, dict[str, dict]]:
    """Return {filename: {metric: {p50, p90, p99, p999}}}."""
    data: dict[str, dict[str, dict]] = {}
    for csv_path in sorted(reports_dir.glob("*.csv")):
        if csv_path.name == "benchmark_summary.csv":
            continue
        file_data: dict[str, dict] = {}
        try:
            with csv_path.open(newline="") as f:
                for row in csv.DictReader(f):
                    metric = row.get("metric", "")
                    try:
                        file_data[metric] = {
                            "p50":  float(row.get("p50",   0) or 0),
                            "p90":  float(row.get("p90",   0) or 0),
                            "p99":  float(row.get("p99",   0) or 0),
                            "p999": float(row.get("p99.9", 0) or 0),
                            "n":    int(row.get("sample_count", 0) or 0),
                        }
                    except ValueError:
                        pass
        except OSError as exc:
            print(f"[warn] {csv_path}: {exc}", file=sys.stderr)
            continue
        if file_data:
            data[csv_path.stem] = file_data
    return data


def _ns_to_us(val: float) -> float:
    return val / 1_000.0


def plot_bar(data: dict, out_dir: Path, show: bool) -> None:
    """Side-by-side bar chart: async vs threaded drift percentiles."""
    import matplotlib.pyplot as plt
    import numpy as np

    # Collect runs that have drift_human and drift_bot
    runs = {
        stem: metrics
        for stem, metrics in data.items()
        if "drift_human" in metrics and "drift_bot" in metrics
    }
    if not runs:
        print("  [plot_bar] no runs with drift data — skipping bar chart")
        return

    percentile_labels = ["p50", "p90", "p99", "p999"]
    x = np.arange(len(percentile_labels))
    width = 0.8 / max(len(runs) * 2, 1)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=False)
    fig.suptitle("Scheduling Drift — Async vs Threaded (µs)", fontsize=13)

    for priority, ax in zip(("drift_human", "drift_bot"), axes):
        ax.set_title("Human edits" if priority == "drift_human" else "Bot edits")
        ax.set_xlabel("Percentile")
        ax.set_ylabel("Drift (µs)")
        ax.set_xticks(x)
        ax.set_xticklabels(percentile_labels)

        offsets = np.linspace(-(len(runs) - 1) * width / 2,
                              (len(runs) - 1) * width / 2, len(runs))
        for offset, (stem, metrics) in zip(offsets, runs.items()):
            values = [_ns_to_us(metrics[priority][p]) for p in percentile_labels]
            ax.bar(x + offset, values, width, label=stem)
        ax.legend(fontsize=8)

    fig.tight_layout()
    out_path = out_dir / "latency_bar.png"
    fig.savefig(out_path, dpi=150)
    print(f"  Saved: {out_path}")
    if show:
        plt.show()
    plt.close(fig)


def plot_cdf(data: dict, out_dir: Path, show: bool) -> None:
    """Empirical CDF from the four p-values available per metric."""
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.set_title("Empirical CDF — Scheduling Drift (µs)")
    ax.set_xlabel("Drift (µs)")
    ax.set_ylabel("Cumulative probability")
    ax.set_xscale("log")

    plotted = 0
    for stem, metrics in data.items():
        for metric in ("drift_human", "drift_bot"):
            if metric not in metrics:
                continue
            m = metrics[metric]
            # Approximate CDF from the four quantile points we have.
            xs = [_ns_to_us(m[p]) for p in ("p50", "p90", "p99", "p999")]
            ys = [0.50, 0.90, 0.99, 0.999]
            # Filter zeros (unsampled histograms).
            pairs = [(x, y) for x, y in zip(xs, ys) if x > 0]
            if len(pairs) < 2:
                continue
            xs_f, ys_f = zip(*pairs)
            label = f"{stem} / {metric.replace('drift_', '')}"
            ax.plot(xs_f, ys_f, marker="o", label=label)
            plotted += 1

    if plotted == 0:
        print("  [plot_cdf] no drift data — skipping CDF chart")
        plt.close(fig)
        return

    ax.axhline(0.99, color="grey", linestyle="--", linewidth=0.8, label="p99")
    ax.legend(fontsize=8)
    ax.grid(True, which="both", linestyle=":", linewidth=0.5)
    fig.tight_layout()

    out_path = out_dir / "latency_cdf.png"
    fig.savefig(out_path, dpi=150)
    print(f"  Saved: {out_path}")
    if show:
        plt.show()
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--reports-dir", default="reports/csv")
    parser.add_argument("--out-dir",     default="reports/plots")
    parser.add_argument("--no-show",     action="store_true",
                        help="Do not open an interactive window (CI-friendly)")
    args = parser.parse_args()

    _require_matplotlib()

    reports_dir = Path(args.reports_dir)
    out_dir     = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=== Latency Plotter ===")
    data = load_csv_metrics(reports_dir)
    if not data:
        print(
            f"\nNo pipeline CSV files found in {reports_dir}.\n"
            "Run a pipeline with --metrics-path first:\n"
            "  cargo run -p rts-cli -- run-async --duration 30s "
            "--metrics-path reports/csv/run1_async\n"
            "  cargo run -p rts-cli -- run-threaded --duration 30s "
            "--metrics-path reports/csv/run1_threaded"
        )
        return

    show = not args.no_show
    print(f"\nLoaded {len(data)} CSV file(s): {', '.join(data)}")
    plot_bar(data, out_dir, show)
    plot_cdf(data, out_dir, show)
    print("\nDone.")


if __name__ == "__main__":
    main()
