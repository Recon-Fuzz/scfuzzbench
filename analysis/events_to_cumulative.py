#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
from typing import Iterable, List, Tuple


REQUIRED_EVENT_COLS = {
    "run_id",
    "instance_id",
    "fuzzer",
    "elapsed_seconds",
}


def die(msg: str) -> None:
    raise SystemExit(f"error: {msg}")


def load_events_csv(path: Path) -> List[dict]:
    with path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            die("events CSV has no header")
        missing = REQUIRED_EVENT_COLS - set(reader.fieldnames)
        if missing:
            die(f"events CSV missing columns: {sorted(missing)}")
        events = []
        for row in reader:
            events.append(row)
    return events


def build_cumulative_rows(events: Iterable[dict], include_zero: bool) -> List[Tuple[str, str, float, int]]:
    grouped: dict[Tuple[str, str], List[float]] = {}
    for event in events:
        fuzzer = str(event["fuzzer"])
        run_id = f"{event['run_id']}:{event['instance_id']}"
        try:
            elapsed = float(event["elapsed_seconds"])
        except (TypeError, ValueError):
            continue
        grouped.setdefault((fuzzer, run_id), []).append(elapsed)

    rows: List[Tuple[str, str, float, int]] = []
    for (fuzzer, run_id), times in grouped.items():
        times_sorted = sorted(times)
        count = 0
        if include_zero:
            rows.append((fuzzer, run_id, 0.0, 0))
        for t in times_sorted:
            count += 1
            rows.append((fuzzer, run_id, t / 3600.0, count))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert events CSV to cumulative bug-count CSV.")
    parser.add_argument("--events-csv", type=Path, required=True)
    parser.add_argument("--out-csv", type=Path, required=True)
    parser.add_argument("--no-zero", action="store_true", help="Do not emit an initial time=0 row.")
    args = parser.parse_args()

    events = load_events_csv(args.events_csv)
    rows = build_cumulative_rows(events, include_zero=not args.no_zero)

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.out_csv.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["fuzzer", "run_id", "time_hours", "bugs_found"])
        for fuzzer, run_id, time_hours, bugs_found in rows:
            writer.writerow([fuzzer, run_id, f"{time_hours:.6f}", bugs_found])

    print(f"wrote {args.out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
