#!/usr/bin/env python3
import argparse
from collections import deque
from pathlib import Path
import sys


def tail_lines(path: Path, count: int) -> str:
    lines: deque[str] = deque(maxlen=count)
    try:
        with path.open("r", errors="ignore") as handle:
            for line in handle:
                lines.append(line.rstrip("\n"))
    except OSError as exc:
        return f"(error reading log: {exc})"
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Print log paths and tails.")
    parser.add_argument("--logs-dir", required=True, type=Path)
    parser.add_argument("--lines", type=int, default=5)
    args = parser.parse_args()

    if not args.logs_dir.exists():
        print(f"Missing logs dir: {args.logs_dir}")
        return 1

    for top in sorted(p for p in args.logs_dir.iterdir() if p.is_dir()):
        log_files = sorted(top.rglob("*.log"))
        print(f"=== {top.name} ===")
        if not log_files:
            print("(no .log files found)")
            continue
        for log_path in log_files:
            size = log_path.stat().st_size if log_path.exists() else 0
            print(f"{log_path} ({size} bytes)")
            tail = tail_lines(log_path, args.lines)
            if tail:
                print(tail)
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
