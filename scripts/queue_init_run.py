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


def get_attr_n(item: dict | None, name: str, default: int = 0) -> int:
    if not isinstance(item, dict):
        return default
    attr = item.get(name)
    if not isinstance(attr, dict):
        return default
    raw = attr.get("N")
    if not isinstance(raw, str):
        return default
    try:
        return int(raw)
    except Exception:
        return default


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


def query_run_shards(table_name: str, run_pk: str) -> list[dict]:
    items: list[dict] = []
    start_key: dict | None = None
    while True:
        args = [
            "dynamodb",
            "query",
            "--table-name",
            table_name,
            "--key-condition-expression",
            "pk = :pk AND begins_with(sk, :prefix)",
            "--expression-attribute-values",
            json.dumps(
                {
                    ":pk": {"S": run_pk},
                    ":prefix": {"S": "SHARD#"},
                },
                separators=(",", ":"),
            ),
            "--consistent-read",
            "--output",
            "json",
        ]
        if start_key is not None:
            args.extend(["--exclusive-start-key", json.dumps(start_key, separators=(",", ":"))])
        proc = aws(args)
        payload = json.loads(proc.stdout or "{}")
        page = payload.get("Items", [])
        if isinstance(page, list):
            items.extend(page)
        start_key = payload.get("LastEvaluatedKey")
        if not isinstance(start_key, dict):
            break
    return items


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


def mark_shard_failed_on_bootstrap(
    table_name: str,
    run_pk: str,
    shard_key: str,
    error_message: str,
) -> bool:
    now = now_iso()
    tx_payload = {
        "TransactItems": [
            {
                "Update": {
                    "TableName": table_name,
                    "Key": {
                        "pk": {"S": run_pk},
                        "sk": {"S": f"SHARD#{shard_key}"},
                    },
                    "ConditionExpression": "#status = :launching OR #status = :queued OR #status = :retrying",
                    "UpdateExpression": (
                        "SET #status = :failed, updated_at = :now, last_error = :error_message, "
                        "last_exit_code = :exit_code, attempts = if_not_exists(attempts, :zero) + :one"
                    ),
                    "ExpressionAttributeNames": {"#status": "status"},
                    "ExpressionAttributeValues": {
                        ":launching": {"S": "launching"},
                        ":queued": {"S": "queued"},
                        ":retrying": {"S": "retrying"},
                        ":failed": {"S": "failed"},
                        ":now": {"S": now},
                        ":error_message": {"S": error_message},
                        ":exit_code": {"N": "901"},
                        ":zero": {"N": "0"},
                        ":one": {"N": "1"},
                    },
                }
            },
            {
                "Update": {
                    "TableName": table_name,
                    "Key": {"pk": {"S": run_pk}, "sk": {"S": "META"}},
                    "UpdateExpression": "ADD failed_count :one SET updated_at = :now",
                    "ExpressionAttributeValues": {
                        ":one": {"N": "1"},
                        ":now": {"S": now},
                    },
                }
            },
        ]
    }
    proc = aws(
        [
            "dynamodb",
            "transact-write-items",
            "--transact-items",
            json.dumps(tx_payload, separators=(",", ":")),
        ],
        check=False,
    )
    if proc.returncode == 0:
        return True
    if "ConditionalCheckFailedException" in proc.stderr or "ConditionalCheckFailed" in proc.stderr:
        return False
    raise RuntimeError(proc.stderr.strip())


def set_run_failed_on_bootstrap(table_name: str, run_pk: str, reason: str) -> bool:
    now = now_iso()
    names = {"#status": "status"}
    values = {
        ":running": {"S": "running"},
        ":failed": {"S": "failed"},
        ":now": {"S": now},
        ":reason": {"S": reason},
    }
    proc = aws(
        [
            "dynamodb",
            "update-item",
            "--table-name",
            table_name,
            "--key",
            json.dumps({"pk": {"S": run_pk}, "sk": {"S": "META"}}, separators=(",", ":")),
            "--condition-expression",
            "#status = :running OR #status = :failed",
            "--update-expression",
            "SET #status = :failed, completed_at = :now, updated_at = :now, last_error = :reason",
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


def recover_bootstrap_failure(table_name: str, run_pk: str, error_message: str) -> tuple[int, bool]:
    recovered_failed = 0
    for item in query_run_shards(table_name, run_pk):
        status = get_attr_s(item, "status")
        if status in TERMINAL_SHARD_STATUSES:
            continue
        shard_key = get_attr_s(item, "shard_key")
        if not shard_key:
            shard_sk = get_attr_s(item, "sk")
            if shard_sk.startswith("SHARD#"):
                shard_key = shard_sk[len("SHARD#") :]
        if not shard_key:
            continue
        reason = f"bootstrap_fatal: {error_message}"
        if mark_shard_failed_on_bootstrap(table_name, run_pk, shard_key, reason):
            recovered_failed += 1

    run_set_failed = set_run_failed_on_bootstrap(table_name, run_pk, f"bootstrap_fatal: {error_message}")
    return recovered_failed, run_set_failed


def write_summary(summary_path: Path | None, summary: dict) -> None:
    if summary_path is None:
        return
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize run state and enqueue shard messages.")
    parser.add_argument("--queue-url", required=True)
    parser.add_argument("--run-state-table", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--benchmark-uuid", required=True)
    parser.add_argument("--max-parallel-effective", required=True, type=int)
    parser.add_argument("--shards-json", required=True, type=Path)
    parser.add_argument("--summary-path", required=False, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_pk = ""
    summary = {
        "run_id": args.run_id,
        "benchmark_uuid": args.benchmark_uuid,
        "requested_shards": 0,
        "run_meta_created": False,
        "shard_rows_created": 0,
        "enqueued_now": 0,
        "already_enqueued": 0,
        "already_running_or_terminal": 0,
        "bootstrap_marked_failed": 0,
        "enqueue_errors": 0,
        "any_enqueue_attempted": False,
        "any_enqueue_succeeded": False,
        "recovery_marked_failed": 0,
        "recovery_set_run_failed": False,
        "recovery_error": "",
        "completed": False,
        "error": "",
    }

    try:
        shards = json.loads(args.shards_json.read_text())
        if not isinstance(shards, list):
            raise RuntimeError("--shards-json must contain a JSON list")

        run_pk = f"RUN#{args.run_id}#{args.benchmark_uuid}"
        created_at = now_iso()
        summary["requested_shards"] = len(shards)

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
        summary["run_meta_created"] = run_created
        if run_created:
            print(f"Initialized run metadata for {run_pk}")
        else:
            print(f"Run metadata already exists for {run_pk}; continuing idempotently")

        shard_rows_created = 0
        enqueued = 0
        already_enqueued = 0
        already_running_or_terminal = 0
        bootstrap_marked_failed = 0
        enqueue_errors = 0

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
            summary["any_enqueue_attempted"] = True
            try:
                message_id = send_shard_message_with_retry(args.queue_url, body)
                summary["any_enqueue_succeeded"] = True
                marked_queued = mark_shard_queued(args.run_state_table, run_pk, shard_key, message_id)
                if marked_queued:
                    enqueued += 1
                else:
                    # Another worker may have already advanced the shard state; duplicate delivery
                    # is guarded by conditional shard-claim transitions in queue workers.
                    already_running_or_terminal += 1
            except Exception as exc:
                enqueue_errors += 1
                reason = f"bootstrap_enqueue_failed: {exc}"
                print(
                    f"enqueue failed for shard {shard_key}; marking shard failed in run-state ({reason})",
                    file=sys.stderr,
                )
                if mark_shard_failed_on_bootstrap(args.run_state_table, run_pk, shard_key, reason):
                    bootstrap_marked_failed += 1
                else:
                    already_running_or_terminal += 1

        summary["shard_rows_created"] = shard_rows_created
        summary["enqueued_now"] = enqueued
        summary["already_enqueued"] = already_enqueued
        summary["already_running_or_terminal"] = already_running_or_terminal
        summary["bootstrap_marked_failed"] = bootstrap_marked_failed
        summary["enqueue_errors"] = enqueue_errors
        summary["completed"] = True
        write_summary(args.summary_path, summary)

        print(
            "Queue bootstrap complete: "
            f"rows_created={shard_rows_created}, "
            f"enqueued_now={enqueued}, "
            f"already_enqueued={already_enqueued}, "
            f"already_running_or_terminal={already_running_or_terminal}, "
            f"bootstrap_marked_failed={bootstrap_marked_failed}, "
            f"enqueue_errors={enqueue_errors}"
        )
        return 0
    except Exception as exc:
        summary["error"] = str(exc)
        if run_pk:
            recovery_errors: list[str] = []
            try:
                recovered_failed, run_set_failed = recover_bootstrap_failure(
                    args.run_state_table,
                    run_pk,
                    str(exc),
                )
                summary["recovery_marked_failed"] = recovered_failed
                summary["recovery_set_run_failed"] = run_set_failed
            except Exception as recovery_exc:
                recovery_errors.append(str(recovery_exc))
            if recovery_errors:
                summary["recovery_error"] = "; ".join(recovery_errors)
        write_summary(args.summary_path, summary)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
