#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wide_csv", type=Path, required=True)
    parser.add_argument("--out_csv", type=Path, required=True)
    args = parser.parse_args()

    wide = pd.read_csv(args.wide_csv)
    if "time_hours" not in wide.columns:
        raise SystemExit("error: expected a time_hours column")

    rows = []
    for column in wide.columns:
        if column == "time_hours":
            continue
        if "_run" not in column:
            continue
        fuzzer, run = column.split("_run", 1)
        run_id = run.strip()
        tmp = pd.DataFrame(
            {
                "fuzzer": fuzzer,
                "run_id": run_id,
                "time_hours": wide["time_hours"],
                "bugs_found": wide[column],
            }
        )
        rows.append(tmp)

    if not rows:
        raise SystemExit("error: no columns matched '*_run*' pattern")

    out = pd.concat(rows, ignore_index=True)
    out.to_csv(args.out_csv, index=False)
    print(f"wrote {args.out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
