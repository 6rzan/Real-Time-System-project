#!/usr/bin/env python3
"""P12 — Sync-Primitive Shootout Plotter

Reads Criterion `estimates.json` files produced by the `sync_shootout` bench
and generates:
  1. reports/plots/shootout_throughput.png  — throughput (ops/s) vs thread count
  2. reports/plots/shootout_scaling.png     — relative scaling vs single-thread

Requires: matplotlib, numpy  (pip install matplotlib numpy)

Usage:
    python scripts/plot_shootout.py [--criterion-dir target/criterion]
                                    [--out-dir       reports/plots]
                                    [--no-show]
"""

import argparse
import json
import sys
from pathlib import Path

SHOOTOUT_GROUPS = [
    "std_mutex",
    "pl_mutex",
    "crossbeam_mpsc",
    "dashmap",
    "drop_oldest_ring",
]

DISPLAY_NAMES = {
    "std_mutex":        "std::Mutex",
    "pl_mutex":         "parking_lot::Mutex",
    "crossbeam_mpsc":   "crossbeam channel",
    "dashmap":          "DashMap",
    "drop_oldest_ring": "DropOldestRing",
}


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


def load_shootout_data(criterion_dir: Path) -> dict[str, dict[int, float]]:
    """Return {group: {threads: mean_ns}}."""
    result: dict[str, dict[int, float]] = {}
    for group in SHOOTOUT_GROUPS:
        group_dir = criterion_dir / group
        if not group_dir.is_dir():
            continue
        thread_data: dict[int, float] = {}
        for param_dir in group_dir.iterdir():
            if not param_dir.is_dir():
                continue
            try:
                threads = int(param_dir.name)
            except ValueError:
                continue
            est_path = param_dir / "estimates.json"
            if not est_path.exists():
                continue
            try:
                with est_path.open() as f:
                    data = json.load(f)
                mean_ns = data["mean"]["point_estimate"]
                thread_data[threads] = mean_ns
            except (KeyError, json.JSONDecodeError, OSError) as exc:
                print(f"  [warn] {est_path}: {exc}", file=sys.stderr)
        if thread_data:
            result[group] = dict(sorted(thread_data.items()))
    return result


OPS_PER_THREAD = 1_000  # must match the bench constant


def mean_ns_to_ops_per_sec(mean_ns: float, threads: int) -> float:
    """Convert mean wall-clock ns for the full batch to ops/s."""
    total_ops = threads * OPS_PER_THREAD
    if mean_ns <= 0:
        return 0.0
    return total_ops / (mean_ns * 1e-9)


def plot_throughput(data: dict, out_dir: Path, show: bool) -> None:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title("Sync-Primitive Throughput vs Thread Count")
    ax.set_xlabel("Contending threads")
    ax.set_ylabel("Throughput (ops / s)")

    for group, thread_data in data.items():
        xs = sorted(thread_data)
        ys = [mean_ns_to_ops_per_sec(thread_data[t], t) for t in xs]
        ax.plot(xs, ys, marker="o", label=DISPLAY_NAMES.get(group, group))

    ax.set_xticks([1, 2, 4, 8, 16])
    ax.legend()
    ax.grid(True, linestyle=":", linewidth=0.5)
    fig.tight_layout()

    out_path = out_dir / "shootout_throughput.png"
    fig.savefig(out_path, dpi=150)
    print(f"  Saved: {out_path}")
    if show:
        plt.show()
    plt.close(fig)


def plot_scaling(data: dict, out_dir: Path, show: bool) -> None:
    """Relative throughput normalised to the single-thread measurement."""
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title("Sync-Primitive Relative Scaling (1-thread = 1.0×)")
    ax.set_xlabel("Contending threads")
    ax.set_ylabel("Relative throughput")

    for group, thread_data in data.items():
        xs = sorted(thread_data)
        if 1 not in thread_data:
            continue
        base = mean_ns_to_ops_per_sec(thread_data[1], 1)
        if base == 0:
            continue
        ys = [mean_ns_to_ops_per_sec(thread_data[t], t) / base for t in xs]
        ax.plot(xs, ys, marker="o", label=DISPLAY_NAMES.get(group, group))

    # Ideal linear scaling reference
    ax.plot([1, 2, 4, 8, 16], [1, 2, 4, 8, 16], "k--", linewidth=0.8,
            label="ideal linear")

    ax.set_xticks([1, 2, 4, 8, 16])
    ax.legend()
    ax.grid(True, linestyle=":", linewidth=0.5)
    fig.tight_layout()

    out_path = out_dir / "shootout_scaling.png"
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
    parser.add_argument("--criterion-dir", default="target/criterion")
    parser.add_argument("--out-dir",       default="reports/plots")
    parser.add_argument("--no-show",       action="store_true",
                        help="Do not open an interactive window (CI-friendly)")
    args = parser.parse_args()

    _require_matplotlib()

    criterion_dir = Path(args.criterion_dir)
    out_dir       = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=== Shootout Plotter ===")
    data = load_shootout_data(criterion_dir)

    if not data:
        print(
            f"\nNo sync_shootout data found under {criterion_dir}.\n"
            "Run the benchmark first:\n"
            "  cargo bench -p rts-bench --bench sync_shootout"
        )
        return

    print(f"\nLoaded groups: {', '.join(data)}")
    show = not args.no_show
    plot_throughput(data, out_dir, show)
    plot_scaling(data, out_dir, show)
    print("\nDone.")


if __name__ == "__main__":
    main()
