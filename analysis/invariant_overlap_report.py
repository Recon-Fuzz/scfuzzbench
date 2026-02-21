#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REQUIRED_COLS = {"fuzzer", "event", "elapsed_seconds"}
OPTIONAL_ID_COLS = ("run_id", "instance_id")


def die(message: str) -> None:
    raise SystemExit(f"error: {message}")


@dataclass(frozen=True)
class InvariantSummary:
    fuzzers: Tuple[str, ...]
    first_seen_seconds: Dict[str, float]
    runs_hit: Dict[str, int]


@dataclass(frozen=True)
class OverlapResult:
    fuzzers: List[str]
    total_events: int
    filtered_events: int
    invariants: Dict[str, InvariantSummary]
    intersections: Dict[Tuple[str, ...], List[str]]
    set_sizes: Dict[str, int]


def load_events(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = sorted(REQUIRED_COLS - set(df.columns))
    if missing:
        die(f"events CSV missing columns: {missing}")

    for col in OPTIONAL_ID_COLS:
        if col not in df.columns:
            df[col] = "unknown"

    df["fuzzer"] = df["fuzzer"].astype(str).str.strip()
    df["event"] = df["event"].astype(str).str.strip()
    df["run_id"] = df["run_id"].astype(str).str.strip()
    df["instance_id"] = df["instance_id"].astype(str).str.strip()
    df["elapsed_seconds"] = pd.to_numeric(df["elapsed_seconds"], errors="coerce")

    df = df[df["fuzzer"] != ""]
    df = df[df["event"] != ""]
    df = df[df["elapsed_seconds"].notna()]
    return df.reset_index(drop=True)


def filter_budget(df: pd.DataFrame, budget_hours: Optional[float]) -> pd.DataFrame:
    if budget_hours is None:
        return df
    if budget_hours < 0:
        die("budget-hours must be >= 0")
    budget_seconds = budget_hours * 3600.0
    return df[df["elapsed_seconds"] <= budget_seconds].reset_index(drop=True)


def build_overlap(df: pd.DataFrame, *, total_events: int) -> OverlapResult:
    first_seen: Dict[str, Dict[str, float]] = defaultdict(dict)
    runs_hit: Dict[str, Dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    set_membership: Dict[str, set[str]] = defaultdict(set)

    for row in df.itertuples(index=False):
        fuzzer = str(row.fuzzer)
        invariant = str(row.event)
        elapsed = float(row.elapsed_seconds)
        run_key = f"{row.run_id}:{row.instance_id}"

        set_membership[fuzzer].add(invariant)
        prev = first_seen[invariant].get(fuzzer)
        if prev is None or elapsed < prev:
            first_seen[invariant][fuzzer] = elapsed
        runs_hit[invariant][fuzzer].add(run_key)

    fuzzers = sorted(set_membership.keys())
    set_sizes = {fuzzer: len(set_membership[fuzzer]) for fuzzer in fuzzers}

    invariants: Dict[str, InvariantSummary] = {}
    intersections: Dict[Tuple[str, ...], List[str]] = defaultdict(list)
    for invariant in sorted(first_seen.keys()):
        inv_fuzzers = tuple(sorted(first_seen[invariant].keys()))
        first = {fuzzer: first_seen[invariant][fuzzer] for fuzzer in inv_fuzzers}
        hits = {fuzzer: len(runs_hit[invariant][fuzzer]) for fuzzer in inv_fuzzers}
        summary = InvariantSummary(
            fuzzers=inv_fuzzers,
            first_seen_seconds=first,
            runs_hit=hits,
        )
        invariants[invariant] = summary
        intersections[inv_fuzzers].append(invariant)

    sorted_intersections: Dict[Tuple[str, ...], List[str]] = {}
    for combo in sorted(intersections.keys()):
        sorted_intersections[combo] = sorted(intersections[combo])

    return OverlapResult(
        fuzzers=fuzzers,
        total_events=total_events,
        filtered_events=len(df),
        invariants=invariants,
        intersections=sorted_intersections,
        set_sizes=set_sizes,
    )


def write_csv_report(result: OverlapResult, out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fuzzers = result.fuzzers
    header = (
        ["invariant", "fuzzers", "fuzzers_count"]
        + [f"{fuzzer}_first_seen_s" for fuzzer in fuzzers]
        + [f"{fuzzer}_runs_hit" for fuzzer in fuzzers]
    )

    with out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)

        rows = sorted(
            result.invariants.items(),
            key=lambda item: (-len(item[1].fuzzers), item[0]),
        )
        for invariant, summary in rows:
            row: List[str] = [invariant, ",".join(summary.fuzzers), str(len(summary.fuzzers))]
            for fuzzer in fuzzers:
                value = summary.first_seen_seconds.get(fuzzer)
                row.append("" if value is None else f"{value:.3f}")
            for fuzzer in fuzzers:
                hits = summary.runs_hit.get(fuzzer)
                row.append("" if hits is None else str(hits))
            writer.writerow(row)


def render_invariant_list(
    lines: List[str], invariants: List[str], *, max_items: int
) -> None:
    if not invariants:
        lines.append("_None._")
        lines.append("")
        return

    shown = invariants[:max_items]
    for invariant in shown:
        lines.append(f"- `{invariant}`")
    if len(invariants) > max_items:
        lines.append(
            f"- _...and {len(invariants) - max_items} more (see `broken_invariants.csv`)._"
        )
    lines.append("")


def write_md_report(
    result: OverlapResult,
    out_md: Path,
    *,
    budget_hours: Optional[float],
    top_k: int,
    max_items_per_group: int = 200,
) -> None:
    out_md.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = []
    lines.append("# Broken invariants")
    lines.append("")
    if budget_hours is None:
        lines.append("- Budget filter: **disabled**")
    else:
        lines.append(f"- Budget filter: **{budget_hours:.2f}h**")
    lines.append(f"- Events considered: **{result.filtered_events} / {result.total_events}**")
    lines.append(f"- Unique invariants: **{len(result.invariants)}**")
    lines.append("")

    if not result.invariants:
        lines.append("No broken invariants were found in the filtered event stream.")
        lines.append("")
        out_md.write_text("\n".join(lines), encoding="utf-8")
        return

    lines.append("## Per-fuzzer totals")
    lines.append("")
    lines.append("| Fuzzer | Invariants |")
    lines.append("|---|---:|")
    for fuzzer in result.fuzzers:
        lines.append(f"| {fuzzer} | {result.set_sizes.get(fuzzer, 0)} |")
    lines.append("")

    all_combo = tuple(result.fuzzers)
    shared_all = (
        len(result.intersections.get(all_combo, []))
        if len(result.fuzzers) > 1
        else len(next(iter(result.intersections.values())))
    )
    lines.append("## High-level overlap")
    lines.append("")
    lines.append(f"- Shared by all fuzzers: **{shared_all}**")
    for fuzzer in result.fuzzers:
        count = len(result.intersections.get((fuzzer,), []))
        lines.append(f"- Exclusive to `{fuzzer}`: **{count}**")
    lines.append("")

    lines.append("## Grouped invariants")
    lines.append("")

    for fuzzer in result.fuzzers:
        invariants = result.intersections.get((fuzzer,), [])
        lines.append("<details>")
        lines.append(f"<summary>Exclusive to <code>{fuzzer}</code> ({len(invariants)})</summary>")
        lines.append("")
        render_invariant_list(lines, invariants, max_items=max_items_per_group)
        lines.append("</details>")
        lines.append("")

    if len(result.fuzzers) > 1:
        invariants = result.intersections.get(all_combo, [])
        lines.append("<details>")
        lines.append(
            f"<summary>Shared by all fuzzers ({len(invariants)})</summary>"
        )
        lines.append("")
        render_invariant_list(lines, invariants, max_items=max_items_per_group)
        lines.append("</details>")
        lines.append("")

    subset_entries: List[Tuple[Tuple[str, ...], List[str]]] = []
    for combo, invariants in result.intersections.items():
        if len(combo) <= 1 or len(combo) == len(result.fuzzers):
            continue
        subset_entries.append((combo, invariants))

    subset_entries.sort(key=lambda item: (-len(item[1]), item[0]))
    subset_entries = subset_entries[: max(top_k, 1)]

    if subset_entries:
        lines.append(f"Top shared subsets (top {len(subset_entries)} by size):")
        lines.append("")
        for combo, invariants in subset_entries:
            combo_label = ", ".join(combo)
            lines.append("<details>")
            lines.append(
                f"<summary><code>{combo_label}</code> ({len(invariants)})</summary>"
            )
            lines.append("")
            render_invariant_list(lines, invariants, max_items=max_items_per_group)
            lines.append("</details>")
            lines.append("")

    out_md.write_text("\n".join(lines), encoding="utf-8")


def write_placeholder_plot(title: str, outpath: Path, message: str) -> None:
    plt.figure(figsize=(10, 5))
    plt.title(title)
    plt.axis("off")
    plt.text(0.5, 0.5, message, ha="center", va="center", wrap=True)
    plt.tight_layout()
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outpath, dpi=200)
    plt.close()


def plot_upset(result: OverlapResult, out_png: Path, *, top_k: int) -> None:
    out_png.parent.mkdir(parents=True, exist_ok=True)
    if not result.invariants:
        write_placeholder_plot(
            "Invariant overlap (UpSet)",
            out_png,
            "No broken invariants found in the filtered event stream.",
        )
        return

    intersections = sorted(
        result.intersections.items(),
        key=lambda item: (-len(item[1]), item[0]),
    )
    intersections = intersections[: max(top_k, 1)]
    if not intersections:
        write_placeholder_plot(
            "Invariant overlap (UpSet)",
            out_png,
            "No intersections available after filtering.",
        )
        return

    fuzzers = sorted(result.fuzzers, key=lambda fuzzer: (-result.set_sizes[fuzzer], fuzzer))
    y_pos = {fuzzer: idx for idx, fuzzer in enumerate(fuzzers)}

    x = np.arange(len(intersections), dtype=float)
    heights = np.array([len(invariants) for _, invariants in intersections], dtype=float)
    max_height = max(float(np.max(heights)), 1.0)
    top_pad = max(0.5, max_height * 0.08)

    fig_width = max(10.0, 6.0 + len(intersections) * 0.6)
    fig_height = max(6.0, 4.0 + len(fuzzers) * 0.5)
    fig = plt.figure(figsize=(fig_width, fig_height), constrained_layout=True)
    gs = fig.add_gridspec(
        2,
        2,
        width_ratios=[1.3, 4.2],
        height_ratios=[3.2, 2.0],
        wspace=0.2,
        hspace=0.05,
    )

    ax_bars = fig.add_subplot(gs[0, 1])
    ax_matrix = fig.add_subplot(gs[1, 1], sharex=ax_bars)
    ax_sets = fig.add_subplot(gs[1, 0], sharey=ax_matrix)

    ax_bars.bar(x, heights, color="#1f77b4")
    for idx, height in enumerate(heights):
        ax_bars.text(
            idx,
            height + top_pad * 0.25,
            str(int(height)),
            ha="center",
            va="bottom",
            fontsize=8,
        )
    ax_bars.set_ylabel("Intersection size")
    ax_bars.set_ylim(0.0, max_height + top_pad)
    ax_bars.set_xticks(x)
    ax_bars.tick_params(axis="x", labelbottom=False)
    ax_bars.set_xlim(-0.6, len(intersections) - 0.4)
    ax_bars.set_title(
        f"Invariant overlap across fuzzers (top {len(intersections)} exact intersections)"
    )

    y_ticks = np.arange(len(fuzzers), dtype=float)
    for y in y_ticks:
        ax_matrix.scatter(x, np.full_like(x, y), color="#d0d0d0", s=24, zorder=1)

    for xi, (combo, _) in enumerate(intersections):
        ys = sorted(y_pos[fuzzer] for fuzzer in combo)
        ax_matrix.scatter(
            np.full(len(ys), xi, dtype=float),
            np.array(ys, dtype=float),
            color="#222222",
            s=40,
            zorder=3,
        )
        if len(ys) > 1:
            ax_matrix.plot([xi, xi], [ys[0], ys[-1]], color="#222222", linewidth=1.4, zorder=2)

    ax_matrix.set_yticks(y_ticks)
    ax_matrix.set_yticklabels(fuzzers)
    ax_matrix.set_xlabel("Exact intersection (dot matrix)")
    ax_matrix.grid(axis="x", alpha=0.2)
    ax_matrix.set_xlim(-0.6, len(intersections) - 0.4)
    ax_matrix.invert_yaxis()

    set_sizes = [result.set_sizes[fuzzer] for fuzzer in fuzzers]
    ax_sets.barh(y_ticks, set_sizes, color="#7daedb")
    max_set_size = max(max(set_sizes), 1)
    for y, size in zip(y_ticks, set_sizes):
        ax_sets.text(size + max_set_size * 0.03, y, str(size), va="center", ha="left", fontsize=8)
    ax_sets.set_xlabel("Set size")
    ax_sets.set_yticks(y_ticks)
    ax_sets.set_yticklabels([])
    ax_sets.invert_yaxis()
    ax_sets.set_xlim(0, max_set_size * 1.25)

    fig.savefig(out_png, dpi=200)
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build broken-invariant overlap artifacts (CSV + Markdown + UpSet chart)."
    )
    parser.add_argument("--events-csv", type=Path, required=True)
    parser.add_argument("--out-md", type=Path, required=True)
    parser.add_argument("--out-csv", type=Path, required=True)
    parser.add_argument("--out-png", type=Path, required=True)
    parser.add_argument("--budget-hours", type=float, default=None)
    parser.add_argument("--top-k", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.top_k <= 0:
        die("top-k must be > 0")

    events = load_events(args.events_csv)
    total_events = len(events)
    filtered = filter_budget(events, args.budget_hours)

    result = build_overlap(filtered, total_events=total_events)
    write_csv_report(result, args.out_csv)
    write_md_report(
        result,
        args.out_md,
        budget_hours=args.budget_hours,
        top_k=args.top_k,
    )
    plot_upset(result, args.out_png, top_k=args.top_k)

    print(f"wrote: {args.out_csv}")
    print(f"wrote: {args.out_md}")
    print(f"wrote: {args.out_png}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
