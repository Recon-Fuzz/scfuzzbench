#!/usr/bin/env python3
import argparse
from pathlib import Path
import shutil
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect .log files for analysis.")
    parser.add_argument("--unzipped-dir", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()

    if not args.unzipped_dir.exists():
        print(f"Missing unzipped dir: {args.unzipped_dir}")
        return 1

    args.out_dir.mkdir(parents=True, exist_ok=True)
    copied = 0
    for instance_dir in sorted(p for p in args.unzipped_dir.iterdir() if p.is_dir()):
        log_files = list(instance_dir.rglob("*.log"))
        if not log_files:
            continue
        dest_instance = args.out_dir / instance_dir.name
        dest_instance.mkdir(parents=True, exist_ok=True)
        for log_file in log_files:
            shutil.copy2(log_file, dest_instance / log_file.name)
            copied += 1
    print(f"Copied {copied} log file(s) to {args.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
