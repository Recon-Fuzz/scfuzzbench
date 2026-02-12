#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path


class AwsError(RuntimeError):
    pass


def aws(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(["aws", *args], text=True, capture_output=True)
    if check and proc.returncode != 0:
        raise AwsError((proc.stderr or proc.stdout).strip() or f"aws {' '.join(args)} failed")
    return proc


def _is_conditional_check_failed(text: str) -> bool:
    return "ConditionalCheckFailedException" in text or "ConditionalCheckFailed" in text


def emit_output(key: str, value: str) -> None:
    out_path = os.environ.get("GITHUB_OUTPUT", "").strip()
    if out_path:
        with open(out_path, "a", encoding="utf-8") as fh:
            fh.write(f"{key}={value}\n")
    else:
        print(f"{key}={value}")


def ensure_table(table_name: str) -> None:
    describe = aws(["dynamodb", "describe-table", "--table-name", table_name], check=False)
    if describe.returncode == 0:
        return

    aws(
        [
            "dynamodb",
            "create-table",
            "--table-name",
            table_name,
            "--attribute-definitions",
            "AttributeName=lock_name,AttributeType=S",
            "--key-schema",
            "AttributeName=lock_name,KeyType=HASH",
            "--billing-mode",
            "PAY_PER_REQUEST",
        ]
    )
    aws(["dynamodb", "wait", "table-exists", "--table-name", table_name])
    aws(
        [
            "dynamodb",
            "update-time-to-live",
            "--table-name",
            table_name,
            "--time-to-live-specification",
            "Enabled=true,AttributeName=expires_at",
        ],
        check=False,
    )


def acquire_lock(
    table_name: str,
    lock_name: str,
    owner_run_id: str,
    workflow_run_id: str,
    lease_seconds: int,
    max_backoff_seconds: int,
    wait_timeout_seconds: int,
) -> None:
    if lease_seconds < 300:
        raise ValueError(f"Invalid lock lease seconds: {lease_seconds}")
    if max_backoff_seconds < 5:
        raise ValueError(f"Invalid lock max backoff: {max_backoff_seconds}")
    if wait_timeout_seconds < 0:
        raise ValueError(f"Invalid lock wait timeout: {wait_timeout_seconds}")

    deadline_epoch = int(time.time()) + wait_timeout_seconds if wait_timeout_seconds > 0 else None
    attempt = 0

    while True:
        now_epoch = int(time.time())
        if deadline_epoch is not None and now_epoch >= deadline_epoch:
            raise TimeoutError(
                f"Timed out waiting for global lock after {wait_timeout_seconds}s. "
                "Set SCFUZZBENCH_LOCK_ACQUIRE_TIMEOUT_SECONDS=0 for unbounded pending."
            )

        expires_epoch = now_epoch + lease_seconds
        acquired_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_epoch))
        item = {
            "lock_name": {"S": lock_name},
            "owner_run_id": {"S": owner_run_id},
            "acquired_at": {"S": acquired_at},
            "expires_at": {"N": str(expires_epoch)},
            "workflow_run_id": {"S": workflow_run_id},
            "updated_at": {"S": acquired_at},
        }
        cond_vals = {":now": {"N": str(now_epoch)}}

        put = aws(
            [
                "dynamodb",
                "put-item",
                "--table-name",
                table_name,
                "--item",
                json.dumps(item, separators=(",", ":")),
                "--condition-expression",
                "attribute_not_exists(lock_name) OR expires_at < :now",
                "--expression-attribute-values",
                json.dumps(cond_vals, separators=(",", ":")),
            ],
            check=False,
        )
        if put.returncode == 0:
            emit_output("lock_acquired", "true")
            emit_output("lock_lease_seconds", str(lease_seconds))
            emit_output("lock_attempts", str(attempt + 1))
            return

        err_text = (put.stderr or put.stdout or "").strip()
        if "ConditionalCheckFailedException" not in err_text and "ConditionalCheckFailed" not in err_text:
            raise AwsError(f"Failed to acquire lock: {err_text}")

        attempt += 1
        holder = aws(
            [
                "dynamodb",
                "get-item",
                "--table-name",
                table_name,
                "--key",
                json.dumps({"lock_name": {"S": lock_name}}, separators=(",", ":")),
                "--consistent-read",
                "--output",
                "json",
            ],
            check=False,
        )
        holder_run_id = "unknown"
        holder_remaining = "unknown"
        if holder.returncode == 0:
            try:
                payload = json.loads(holder.stdout or "{}")
                item = payload.get("Item", {}) if isinstance(payload, dict) else {}
                holder_run_id = str(item.get("owner_run_id", {}).get("S", "unknown"))
                raw_exp = str(item.get("expires_at", {}).get("N", "0"))
                if raw_exp.isdigit():
                    rem = int(raw_exp) - now_epoch
                    if rem < 0:
                        rem = 0
                    holder_remaining = f"{rem}s"
            except Exception:
                pass

        delay = 2 ** (attempt - 1)
        if delay > max_backoff_seconds:
            delay = max_backoff_seconds
        delay = min(max_backoff_seconds, max(5, delay + random.randint(0, 5)))
        print(
            f"Global lock busy (owner_run_id={holder_run_id}, remaining={holder_remaining}); "
            f"retrying in {delay}s (attempt {attempt}).",
            flush=True,
        )
        time.sleep(delay)


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value))
    except Exception:
        return default


def assess_release_safety(
    summary_path: Path,
    run_metadata_path: Path,
) -> None:
    safe_to_release = True
    reasons: list[str] = []

    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        any_enqueue_succeeded = bool(summary.get("any_enqueue_succeeded", False))
        enqueued_now = _safe_int(summary.get("enqueued_now", 0))
        already_enqueued = _safe_int(summary.get("already_enqueued", 0))
        already_running_or_terminal = _safe_int(summary.get("already_running_or_terminal", 0))
        bootstrap_marked_failed = _safe_int(summary.get("bootstrap_marked_failed", 0))

        if (
            any_enqueue_succeeded
            or enqueued_now > 0
            or already_enqueued > 0
            or already_running_or_terminal > 0
            or bootstrap_marked_failed > 0
        ):
            safe_to_release = False
            reasons.append("queue-init-summary indicates active shard state")
    elif run_metadata_path.exists():
        safe_to_release = False
        reasons.append("missing queue-init-summary after bootstrap attempt; conservatively keep lock")

    if run_metadata_path.exists():
        metadata = json.loads(run_metadata_path.read_text(encoding="utf-8"))
        queue_url = str((metadata.get("queue_url") or {}).get("value", "") or "").strip()
        run_state_table = str((metadata.get("run_state_table_name") or {}).get("value", "") or "").strip()
        run_id = str((metadata.get("run_id") or {}).get("value", "") or "").strip()
        benchmark_uuid = str((metadata.get("benchmark_uuid") or {}).get("value", "") or "").strip()

        if queue_url:
            attrs = aws(
                [
                    "sqs",
                    "get-queue-attributes",
                    "--queue-url",
                    queue_url,
                    "--attribute-names",
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "--output",
                    "json",
                ],
                check=False,
            )
            if attrs.returncode != 0:
                safe_to_release = False
                reasons.append("unable to read queue attributes; conservatively keep lock")
            else:
                payload = json.loads(attrs.stdout or "{}")
                attrs_map = payload.get("Attributes", {}) if isinstance(payload, dict) else {}
                visible = _safe_int(attrs_map.get("ApproximateNumberOfMessages", "0"))
                inflight = _safe_int(attrs_map.get("ApproximateNumberOfMessagesNotVisible", "0"))
                if visible > 0 or inflight > 0:
                    safe_to_release = False
                    reasons.append(f"queue has pending/inflight messages ({visible}/{inflight})")

        if run_state_table and run_id and benchmark_uuid:
            run_pk = f"RUN#{run_id}#{benchmark_uuid}"
            meta = aws(
                [
                    "dynamodb",
                    "get-item",
                    "--table-name",
                    run_state_table,
                    "--key",
                    json.dumps({"pk": {"S": run_pk}, "sk": {"S": "META"}}, separators=(",", ":")),
                    "--consistent-read",
                    "--output",
                    "json",
                ],
                check=False,
            )
            if meta.returncode != 0:
                safe_to_release = False
                reasons.append("unable to read run-state META; conservatively keep lock")
            else:
                payload = json.loads(meta.stdout or "{}")
                item = payload.get("Item", {}) if isinstance(payload, dict) else {}
                run_status = str(item.get("status", {}).get("S", "") or "")
                if run_status == "running":
                    safe_to_release = False
                    reasons.append("run-state META status=running")

    reason_text = "no active queue/run-state evidence" if not reasons else "; ".join(reasons)
    emit_output("safe_to_release", "true" if safe_to_release else "false")
    emit_output("reason", reason_text)


def release_lock(table_name: str, lock_name: str, owner_run_id: str) -> None:
    key = {"lock_name": {"S": lock_name}}
    values = {":owner": {"S": owner_run_id}}
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        proc = aws(
            [
                "dynamodb",
                "delete-item",
                "--table-name",
                table_name,
                "--key",
                json.dumps(key, separators=(",", ":")),
                "--condition-expression",
                "owner_run_id = :owner",
                "--expression-attribute-values",
                json.dumps(values, separators=(",", ":")),
            ],
            check=False,
        )
        if proc.returncode == 0:
            return

        err_text = (proc.stderr or proc.stdout or "").strip()
        if _is_conditional_check_failed(err_text):
            # If ownership changed or lock is already gone, treat release as complete.
            return

        if attempt >= max_attempts:
            raise AwsError(f"Failed to release lock after {max_attempts} attempts: {err_text}")

        delay = min(10, 2 ** (attempt - 1))
        delay = max(1, delay + random.randint(0, 2))
        print(f"Lock release failed (attempt {attempt}/{max_attempts}): {err_text}; retrying in {delay}s", flush=True)
        time.sleep(delay)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark global lock orchestration helper")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ensure = sub.add_parser("ensure-table")
    p_ensure.add_argument("--table-name", required=True)

    p_acquire = sub.add_parser("acquire")
    p_acquire.add_argument("--table-name", required=True)
    p_acquire.add_argument("--lock-name", required=True)
    p_acquire.add_argument("--owner-run-id", required=True)
    p_acquire.add_argument("--workflow-run-id", required=True)
    p_acquire.add_argument("--lease-seconds", required=True, type=int)
    p_acquire.add_argument("--wait-timeout-seconds", required=True, type=int)
    p_acquire.add_argument("--max-backoff-seconds", required=True, type=int)

    p_assess = sub.add_parser("assess-bootstrap-release-safety")
    p_assess.add_argument("--queue-init-summary", default="queue-init-summary.json")
    p_assess.add_argument("--run-metadata", default="run_metadata.json")

    p_release = sub.add_parser("release")
    p_release.add_argument("--table-name", required=True)
    p_release.add_argument("--lock-name", required=True)
    p_release.add_argument("--owner-run-id", required=True)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.cmd == "ensure-table":
            ensure_table(args.table_name)
            return 0
        if args.cmd == "acquire":
            acquire_lock(
                table_name=args.table_name,
                lock_name=args.lock_name,
                owner_run_id=args.owner_run_id,
                workflow_run_id=args.workflow_run_id,
                lease_seconds=args.lease_seconds,
                max_backoff_seconds=args.max_backoff_seconds,
                wait_timeout_seconds=args.wait_timeout_seconds,
            )
            return 0
        if args.cmd == "assess-bootstrap-release-safety":
            assess_release_safety(
                summary_path=Path(args.queue_init_summary),
                run_metadata_path=Path(args.run_metadata),
            )
            return 0
        if args.cmd == "release":
            release_lock(args.table_name, args.lock_name, args.owner_run_id)
            return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
