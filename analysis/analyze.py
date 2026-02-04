#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

LOG_FILE_RE = re.compile(r".+\.log$")
INSTANCE_PREFIX_RE = re.compile(r"^(i-[0-9a-f]+)-(.*)$")
ABS_TS_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} [0-9:.]+)\]")
MEDUSA_ELAPSED_RE = re.compile(r"elapsed:\s*([0-9hms]+)")
FOUNDATION_JSON_RE = re.compile(r"^\s*\{.*\}\s*$")
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")
FALSIFIED_RE = re.compile(r"Test\s+([^\s]+)\s+falsified!")


@dataclass(frozen=True)
class Event:
    run_id: str
    instance_id: str
    fuzzer: str
    fuzzer_label: str
    event: str
    elapsed_seconds: float
    source: str
    log_path: str


def parse_duration(text: str) -> Optional[int]:
    matches = re.findall(r"(\d+)([hms])", text)
    if not matches:
        return None
    total = 0
    for value, unit in matches:
        value_i = int(value)
        if unit == "h":
            total += value_i * 3600
        elif unit == "m":
            total += value_i * 60
        elif unit == "s":
            total += value_i
    return total


def parse_timestamp(line: str) -> Optional[float]:
    match = ABS_TS_RE.match(line)
    if not match:
        return None
    ts = match.group(1)
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def infer_run_id(path: Path) -> Optional[str]:
    for part in path.parts:
        if part.isdigit() and len(part) >= 8:
            return part
    return None


def split_instance_label(label: str) -> Tuple[str, str]:
    match = INSTANCE_PREFIX_RE.match(label)
    if match:
        return match.group(1), match.group(2)
    return "unknown", label


def normalize_fuzzer(fuzzer_label: str) -> str:
    lower = fuzzer_label.lower()
    if "echidna" in lower and "symexec" in lower:
        return "echidna-symexec"
    if lower.startswith("echidna"):
        return "echidna"
    if "medusa" in lower:
        return "medusa"
    if "foundry" in lower:
        return "foundry"
    return fuzzer_label


def extract_bang_event(line: str) -> Optional[str]:
    if "!!!" not in line:
        return None
    _, after = line.split("!!!", 1)
    candidate = after.strip()
    for sep in ("»", "\"", ")"):
        if sep in candidate:
            candidate = candidate.split(sep, 1)[0].strip()
    candidate = candidate.strip()
    if not candidate:
        return None
    return candidate


def parse_foundry_log(
    path: Path, run_id: str, instance_id: str, fuzzer_label: str
) -> List[Event]:
    events: List[Event] = []
    seen = set()
    first_ts: Optional[float] = None
    with path.open("r", errors="ignore") as handle:
        for line in handle:
            if not FOUNDATION_JSON_RE.match(line):
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            invariant = payload.get("invariant")
            if not invariant:
                continue
            failed = payload.get("failed")
            if failed is not None:
                try:
                    failed_value = int(failed)
                except (TypeError, ValueError):
                    failed_value = 0
                if failed_value <= 0:
                    continue
            elif "assertion_failure" not in invariant:
                continue
            ts = payload.get("timestamp")
            if ts is None:
                continue
            if first_ts is None:
                first_ts = float(ts)
            elapsed = float(ts) - first_ts
            if invariant in seen:
                continue
            seen.add(invariant)
            events.append(
                Event(
                    run_id=run_id,
                    instance_id=instance_id,
                    fuzzer=normalize_fuzzer(fuzzer_label),
                    fuzzer_label=fuzzer_label,
                    event=invariant,
                    elapsed_seconds=elapsed,
                    source="foundry-json",
                    log_path=str(path),
                )
            )
    return events


def parse_medusa_log(
    path: Path, run_id: str, instance_id: str, fuzzer_label: str
) -> List[Event]:
    events: List[Event] = []
    seen = set()
    last_elapsed: Optional[int] = None
    last_failed: Optional[str] = None
    with path.open("r", errors="ignore") as handle:
        for line in handle:
            clean_line = ANSI_ESCAPE_RE.sub("", line)
            elapsed_match = MEDUSA_ELAPSED_RE.search(clean_line)
            if elapsed_match:
                last_elapsed = parse_duration(elapsed_match.group(1))

            failed_match = re.search(r"Assertion Test:\s*(.+)$", clean_line)
            if "[FAILED]" in clean_line and failed_match:
                last_failed = failed_match.group(1).strip()
                if last_failed not in seen and last_elapsed is not None:
                    seen.add(last_failed)
                    events.append(
                        Event(
                            run_id=run_id,
                            instance_id=instance_id,
                            fuzzer=normalize_fuzzer(fuzzer_label),
                            fuzzer_label=fuzzer_label,
                            event=last_failed,
                            elapsed_seconds=float(last_elapsed),
                            source="medusa-failed",
                            log_path=str(path),
                        )
                    )
                continue

            bang_event = extract_bang_event(clean_line)
            if bang_event and last_elapsed is not None:
                event_name = last_failed or bang_event
                if event_name not in seen:
                    seen.add(event_name)
                    events.append(
                        Event(
                            run_id=run_id,
                            instance_id=instance_id,
                            fuzzer=normalize_fuzzer(fuzzer_label),
                            fuzzer_label=fuzzer_label,
                            event=event_name,
                            elapsed_seconds=float(last_elapsed),
                            source="medusa-bang",
                            log_path=str(path),
                        )
                    )
                continue

            if "panic: assertion failed" in clean_line and last_failed and last_failed not in seen:
                if last_elapsed is not None:
                    seen.add(last_failed)
                    events.append(
                        Event(
                            run_id=run_id,
                            instance_id=instance_id,
                            fuzzer=normalize_fuzzer(fuzzer_label),
                            fuzzer_label=fuzzer_label,
                            event=last_failed,
                            elapsed_seconds=float(last_elapsed),
                            source="medusa-panic",
                            log_path=str(path),
                        )
                    )
    return events


def parse_generic_log(
    path: Path, run_id: str, instance_id: str, fuzzer_label: str
) -> List[Event]:
    events: List[Event] = []
    seen = set()
    first_ts: Optional[float] = None
    last_ts: Optional[float] = None
    with path.open("r", errors="ignore") as handle:
        for line in handle:
            clean_line = ANSI_ESCAPE_RE.sub("", line)
            ts = parse_timestamp(clean_line)
            if ts is not None:
                last_ts = ts
                if first_ts is None:
                    first_ts = ts
            bang_event = extract_bang_event(clean_line)
            if bang_event:
                if bang_event in seen:
                    continue
                if last_ts is None or first_ts is None:
                    continue
                seen.add(bang_event)
                events.append(
                    Event(
                        run_id=run_id,
                        instance_id=instance_id,
                        fuzzer=normalize_fuzzer(fuzzer_label),
                        fuzzer_label=fuzzer_label,
                        event=bang_event,
                        elapsed_seconds=last_ts - first_ts,
                        source="bang",
                        log_path=str(path),
                    )
                )
                continue
            falsified_match = FALSIFIED_RE.search(clean_line)
            if falsified_match:
                event_name = falsified_match.group(1)
                if event_name in seen:
                    continue
                if last_ts is None or first_ts is None:
                    continue
                seen.add(event_name)
                events.append(
                    Event(
                        run_id=run_id,
                        instance_id=instance_id,
                        fuzzer=normalize_fuzzer(fuzzer_label),
                        fuzzer_label=fuzzer_label,
                        event=event_name,
                        elapsed_seconds=last_ts - first_ts,
                        source="falsified",
                        log_path=str(path),
                    )
                )
                continue
            if "panic: assertion failed" in clean_line or "FAILURE" in clean_line:
                if last_ts is None or first_ts is None:
                    continue
                event_name = "assertion_failed"
                if event_name in seen:
                    continue
                seen.add(event_name)
                events.append(
                    Event(
                        run_id=run_id,
                        instance_id=instance_id,
                        fuzzer=normalize_fuzzer(fuzzer_label),
                        fuzzer_label=fuzzer_label,
                        event=event_name,
                        elapsed_seconds=last_ts - first_ts,
                        source="panic",
                        log_path=str(path),
                    )
                )
    return events


def parse_logs(logs_dir: Path, run_id: Optional[str]) -> List[Event]:
    events: List[Event] = []
    run_id_value = run_id or infer_run_id(logs_dir) or "unknown"
    for path in logs_dir.rglob("*"):
        if not path.is_file():
            continue
        if not LOG_FILE_RE.match(path.name):
            continue
        rel = path.relative_to(logs_dir)
        if len(rel.parts) < 2:
            continue
        instance_label = rel.parts[0]
        instance_id, fuzzer_label = split_instance_label(instance_label)
        fuzzer = normalize_fuzzer(fuzzer_label)
        if fuzzer == "foundry":
            events.extend(parse_foundry_log(path, run_id_value, instance_id, fuzzer_label))
        elif fuzzer == "medusa":
            events.extend(parse_medusa_log(path, run_id_value, instance_id, fuzzer_label))
        else:
            events.extend(parse_generic_log(path, run_id_value, instance_id, fuzzer_label))
    return events


def write_events_csv(events: Iterable[Event], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "run_id",
                "instance_id",
                "fuzzer",
                "fuzzer_label",
                "event",
                "elapsed_seconds",
                "source",
                "log_path",
            ]
        )
        for event in events:
            writer.writerow(
                [
                    event.run_id,
                    event.instance_id,
                    event.fuzzer,
                    event.fuzzer_label,
                    event.event,
                    f"{event.elapsed_seconds:.3f}",
                    event.source,
                    event.log_path,
                ]
            )


def load_events_csv(path: Path) -> List[Event]:
    events: List[Event] = []
    with path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                elapsed = float(row["elapsed_seconds"])
            except (KeyError, ValueError):
                continue
            events.append(
                Event(
                    run_id=row.get("run_id", "unknown"),
                    instance_id=row.get("instance_id", "unknown"),
                    fuzzer=row.get("fuzzer", "unknown"),
                    fuzzer_label=row.get("fuzzer_label", row.get("fuzzer", "unknown")),
                    event=row.get("event", "unknown"),
                    elapsed_seconds=elapsed,
                    source=row.get("source", ""),
                    log_path=row.get("log_path", ""),
                )
            )
    return events


def build_runs(events: Iterable[Event]) -> Dict[str, Dict[str, List[float]]]:
    runs: Dict[str, Dict[str, List[float]]] = {}
    for event in events:
        run_key = f"{event.run_id}:{event.instance_id}:{event.fuzzer_label}"
        runs.setdefault(event.fuzzer, {}).setdefault(run_key, []).append(event.elapsed_seconds)
    for fuzzer_runs in runs.values():
        for run_key, times in fuzzer_runs.items():
            fuzzer_runs[run_key] = sorted(set(times))
    return runs


def build_event_sets(events: Iterable[Event]) -> Dict[str, set]:
    event_sets: Dict[str, set] = defaultdict(set)
    for event in events:
        event_sets[event.fuzzer].add(event.event)
    return event_sets


def compute_exclusive_events(event_sets: Dict[str, set]) -> Tuple[Dict[str, set], Dict[str, set]]:
    event_to_fuzzers: Dict[str, set] = defaultdict(set)
    for fuzzer, events in event_sets.items():
        for event in events:
            event_to_fuzzers[event].add(fuzzer)
    exclusive: Dict[str, set] = {fuzzer: set() for fuzzer in event_sets}
    for event, fuzzers in event_to_fuzzers.items():
        if len(fuzzers) == 1:
            fuzzer = next(iter(fuzzers))
            exclusive[fuzzer].add(event)
    return exclusive, event_to_fuzzers


def write_summary_csv(events: Iterable[Event], out_path: Path) -> None:
    runs = build_runs(events)
    event_sets = build_event_sets(events)
    exclusive, _ = compute_exclusive_events(event_sets)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "fuzzer",
                "runs",
                "unique_bugs",
                "exclusive_bugs",
                "shared_bugs",
                "mean_bugs_per_run",
                "median_bugs_per_run",
                "stdev_bugs_per_run",
                "min_bugs_per_run",
                "max_bugs_per_run",
                "mean_ttfb_seconds",
                "median_ttfb_seconds",
            ]
        )
        for fuzzer in sorted(event_sets.keys() | runs.keys()):
            run_map = runs.get(fuzzer, {})
            run_counts = [len(times) for times in run_map.values()]
            ttfb_values = [min(times) for times in run_map.values() if times]
            unique_bugs = len(event_sets.get(fuzzer, set()))
            exclusive_bugs = len(exclusive.get(fuzzer, set()))
            shared_bugs = unique_bugs - exclusive_bugs
            mean_count = statistics.mean(run_counts) if run_counts else 0.0
            median_count = statistics.median(run_counts) if run_counts else 0.0
            stdev_count = statistics.stdev(run_counts) if len(run_counts) > 1 else 0.0
            min_count = min(run_counts) if run_counts else 0
            max_count = max(run_counts) if run_counts else 0
            mean_ttfb = statistics.mean(ttfb_values) if ttfb_values else 0.0
            median_ttfb = statistics.median(ttfb_values) if ttfb_values else 0.0
            writer.writerow(
                [
                    fuzzer,
                    len(run_map),
                    unique_bugs,
                    exclusive_bugs,
                    shared_bugs,
                    f"{mean_count:.3f}",
                    f"{median_count:.3f}",
                    f"{stdev_count:.3f}",
                    min_count,
                    max_count,
                    f"{mean_ttfb:.3f}",
                    f"{median_ttfb:.3f}",
                ]
            )


def write_overlap_csv(events: Iterable[Event], out_path: Path) -> None:
    event_sets = build_event_sets(events)
    fuzzers = sorted(event_sets.keys())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["fuzzer", *fuzzers])
        for fuzzer in fuzzers:
            row = [fuzzer]
            set_a = event_sets[fuzzer]
            for other in fuzzers:
                set_b = event_sets[other]
                union = set_a | set_b
                jaccard = (len(set_a & set_b) / len(union)) if union else 0.0
                row.append(f"{jaccard:.3f}")
            writer.writerow(row)


def write_exclusive_csv(events: Iterable[Event], out_path: Path) -> None:
    event_sets = build_event_sets(events)
    exclusive, _ = compute_exclusive_events(event_sets)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["fuzzer", "event"])
        for fuzzer in sorted(exclusive.keys()):
            for event in sorted(exclusive[fuzzer]):
                writer.writerow([fuzzer, event])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze scfuzzbench logs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse_parser = subparsers.add_parser("parse", help="Parse logs to CSV.")
    parse_parser.add_argument("--logs-dir", required=True, type=Path)
    parse_parser.add_argument("--run-id", default=None)
    parse_parser.add_argument("--out-csv", required=True, type=Path)

    run_parser = subparsers.add_parser("run", help="Parse logs and write CSVs.")
    run_parser.add_argument("--logs-dir", required=True, type=Path)
    run_parser.add_argument("--run-id", default=None)
    run_parser.add_argument("--out-dir", required=True, type=Path)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "parse":
        events = parse_logs(args.logs_dir, args.run_id)
        write_events_csv(events, args.out_csv)
        return 0
    if args.command == "run":
        out_dir: Path = args.out_dir
        events = parse_logs(args.logs_dir, args.run_id)
        events_csv = out_dir / "events.csv"
        summary_csv = out_dir / "summary.csv"
        overlap_csv = out_dir / "overlap.csv"
        exclusive_csv = out_dir / "exclusive.csv"
        write_events_csv(events, events_csv)
        write_summary_csv(events, summary_csv)
        write_overlap_csv(events, overlap_csv)
        write_exclusive_csv(events, exclusive_csv)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
