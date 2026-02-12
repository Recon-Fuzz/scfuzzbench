#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def aws(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(["aws", *args], text=True, capture_output=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"aws {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc


def conditional_put_item(table_name: str, item: dict) -> bool:
    proc = aws(
        [
            "dynamodb",
            "put-item",
            "--table-name",
            table_name,
            "--item",
            json.dumps(item, separators=(",", ":")),
            "--condition-expression",
            "attribute_not_exists(pk) AND attribute_not_exists(sk)",
        ],
        check=False,
    )
    if proc.returncode == 0:
        return True
    if "ConditionalCheckFailedException" in proc.stderr:
        return False
    raise RuntimeError(proc.stderr.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize run state and enqueue shard messages.")
    parser.add_argument("--queue-url", required=True)
    parser.add_argument("--run-state-table", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--benchmark-uuid", required=True)
    parser.add_argument("--max-parallel-effective", required=True, type=int)
    parser.add_argument("--shards-json", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    shards = json.loads(args.shards_json.read_text())
    if not isinstance(shards, list):
        raise SystemExit("--shards-json must contain a JSON list")

    run_pk = f"RUN#{args.run_id}#{args.benchmark_uuid}"
    created_at = now_iso()

    run_item = {
        "pk": {"S": run_pk},
        "sk": {"S": "META"},
        "entity_type": {"S": "run"},
        "status": {"S": "running"},
        "requested_shards": {"N": str(len(shards))},
        "succeeded_count": {"N": "0"},
        "failed_count": {"N": "0"},
        "max_parallel_effective": {"N": str(args.max_parallel_effective)},
        "created_at": {"S": created_at},
        "updated_at": {"S": created_at},
    }

    run_created = conditional_put_item(args.run_state_table, run_item)
    if run_created:
        print(f"Initialized run metadata for {run_pk}")
    else:
        print(f"Run metadata already exists for {run_pk}; continuing idempotently")

    enqueued = 0
    skipped = 0

    for shard in shards:
        if not isinstance(shard, dict):
            raise RuntimeError(f"Invalid shard entry: {shard!r}")
        shard_key = str(shard.get("shard_key", "")).strip()
        fuzzer_key = str(shard.get("fuzzer_key", "")).strip()
        run_index = shard.get("run_index")
        if not shard_key or not fuzzer_key or not isinstance(run_index, int):
            raise RuntimeError(f"Invalid shard payload: {shard!r}")

        shard_item = {
            "pk": {"S": run_pk},
            "sk": {"S": f"SHARD#{shard_key}"},
            "entity_type": {"S": "shard"},
            "shard_key": {"S": shard_key},
            "fuzzer_key": {"S": fuzzer_key},
            "run_index": {"N": str(run_index)},
            "status": {"S": "queued"},
            "attempts": {"N": "0"},
            "updated_at": {"S": created_at},
        }

        inserted = conditional_put_item(args.run_state_table, shard_item)
        if not inserted:
            skipped += 1
            continue

        body = {
            "shard_key": shard_key,
            "fuzzer_key": fuzzer_key,
            "run_index": run_index,
        }
        aws(
            [
                "sqs",
                "send-message",
                "--queue-url",
                args.queue_url,
                "--message-body",
                json.dumps(body, separators=(",", ":")),
            ]
        )
        enqueued += 1

    print(f"Enqueued {enqueued} shard message(s); skipped {skipped} existing shard(s)")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
