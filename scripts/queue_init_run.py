#!/usr/bin/env python3
import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

TERMINAL_SHARD_STATUSES = {"succeeded", "failed", "timed_out"}
NON_ENQUEUEABLE_SHARD_STATUSES = TERMINAL_SHARD_STATUSES | {"running"}


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


def get_item(table_name: str, key: dict) -> dict | None:
    proc = aws(
        [
            "dynamodb",
            "get-item",
            "--table-name",
            table_name,
            "--key",
            json.dumps(key, separators=(",", ":")),
            "--consistent-read",
            "--output",
            "json",
        ]
    )
    payload = json.loads(proc.stdout or "{}")
    return payload.get("Item")


def get_attr_s(item: dict | None, name: str) -> str:
    if not isinstance(item, dict):
        return ""
    attr = item.get(name)
    if not isinstance(attr, dict):
        return ""
    value = attr.get("S")
    return value if isinstance(value, str) else ""


def mark_shard_queued(table_name: str, run_pk: str, shard_key: str, message_id: str) -> bool:
    sk = f"SHARD#{shard_key}"
    now = now_iso()
    names = {"#status": "status"}
    values = {
        ":launching": {"S": "launching"},
        ":queued": {"S": "queued"},
        ":retrying": {"S": "retrying"},
        ":now": {"S": now},
        ":msg": {"S": message_id},
        ":one": {"N": "1"},
    }
    proc = aws(
        [
            "dynamodb",
            "update-item",
            "--table-name",
            table_name,
            "--key",
            json.dumps({"pk": {"S": run_pk}, "sk": {"S": sk}}, separators=(",", ":")),
            "--condition-expression",
            "#status = :launching OR #status = :queued OR #status = :retrying",
            "--update-expression",
            "SET #status = :queued, updated_at = :now, enqueued_at = :now, last_enqueue_message_id = :msg "
            "ADD enqueue_attempts :one",
            "--expression-attribute-names",
            json.dumps(names, separators=(",", ":")),
            "--expression-attribute-values",
            json.dumps(values, separators=(",", ":")),
        ],
        check=False,
    )
    if proc.returncode == 0:
        return True
    if "ConditionalCheckFailedException" in proc.stderr:
        return False
    raise RuntimeError(proc.stderr.strip())


def send_shard_message(queue_url: str, body: dict) -> str:
    proc = aws(
        [
            "sqs",
            "send-message",
            "--queue-url",
            queue_url,
            "--message-body",
            json.dumps(body, separators=(",", ":")),
            "--output",
            "json",
        ]
    )
    payload = json.loads(proc.stdout or "{}")
    message_id = str(payload.get("MessageId", "")).strip()
    if not message_id:
        raise RuntimeError("sqs send-message succeeded but did not return MessageId")
    return message_id


def send_shard_message_with_retry(queue_url: str, body: dict, max_attempts: int = 5) -> str:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return send_shard_message(queue_url, body)
        except Exception as exc:  # pragma: no cover - transient AWS failures
            last_error = exc
            if attempt >= max_attempts:
                break
            backoff = min(30.0, 2 ** (attempt - 1))
            jitter = random.uniform(0.0, 1.0)
            sleep_seconds = backoff + jitter
            print(
                f"send-message failed for shard {body.get('shard_key')} "
                f"(attempt {attempt}/{max_attempts}): {exc}; retrying in {sleep_seconds:.1f}s",
                file=sys.stderr,
            )
            time.sleep(sleep_seconds)
    raise RuntimeError(f"Failed to enqueue shard after {max_attempts} attempts: {last_error}")


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

    shard_rows_created = 0
    enqueued = 0
    already_enqueued = 0
    already_running_or_terminal = 0

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
            "status": {"S": "launching"},
            "attempts": {"N": "0"},
            "enqueue_attempts": {"N": "0"},
            "updated_at": {"S": created_at},
        }

        inserted = conditional_put_item(args.run_state_table, shard_item)
        if inserted:
            shard_rows_created += 1

        shard_state = get_item(
            args.run_state_table,
            {"pk": {"S": run_pk}, "sk": {"S": f"SHARD#{shard_key}"}},
        )
        if shard_state is None:
            raise RuntimeError(f"Missing shard state row after initialization for {shard_key}")

        status = get_attr_s(shard_state, "status")
        enqueued_at = get_attr_s(shard_state, "enqueued_at")
        if status in NON_ENQUEUEABLE_SHARD_STATUSES:
            already_running_or_terminal += 1
            continue
        if status == "queued" and enqueued_at:
            already_enqueued += 1
            continue

        body = {
            "shard_key": shard_key,
            "fuzzer_key": fuzzer_key,
            "run_index": run_index,
        }
        message_id = send_shard_message_with_retry(args.queue_url, body)
        marked_queued = mark_shard_queued(args.run_state_table, run_pk, shard_key, message_id)
        if marked_queued:
            enqueued += 1
        else:
            # Another worker may have already advanced the shard state; duplicate delivery
            # is guarded by conditional shard-claim transitions in queue workers.
            already_running_or_terminal += 1

    print(
        "Queue bootstrap complete: "
        f"rows_created={shard_rows_created}, "
        f"enqueued_now={enqueued}, "
        f"already_enqueued={already_enqueued}, "
        f"already_running_or_terminal={already_running_or_terminal}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
