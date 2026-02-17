#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import os
import random
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

LOCK_SCRIPT = "/opt/scfuzzbench/scripts/s3_lock.py"
QUEUE_INIT_SCRIPT = "/opt/scfuzzbench/scripts/s3_queue_init.py"


def log(message: str) -> None:
    ts = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {message}", flush=True)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def utc_now() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


def iso_utc(ts: dt.datetime | None = None) -> str:
    if ts is None:
        ts = utc_now()
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def epoch_now() -> int:
    return int(time.time())


def sanitize(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def aws_env() -> dict[str, str]:
    env = os.environ.copy()
    profile = os.environ.get("AWS_PROFILE", "").strip()
    if profile:
        env["AWS_PROFILE"] = profile
    return env


def aws_text(args: list[str]) -> str:
    return subprocess.check_output(["aws", *args], text=True, env=aws_env())


def aws_json(args: list[str]) -> dict[str, Any]:
    out = aws_text([*args, "--output", "json"])
    return json.loads(out) if out.strip() else {}


def list_keys(bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token: str | None = None
    while True:
        cmd = ["s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix]
        if token:
            cmd += ["--continuation-token", token]
        data = aws_json(cmd)
        keys.extend([obj["Key"] for obj in data.get("Contents", [])])
        if not data.get("IsTruncated"):
            break
        token = data.get("NextContinuationToken")
        if not token:
            break
    return keys


def s3_json_or_none(bucket: str, key: str) -> dict[str, Any] | None:
    try:
        raw = aws_text(["s3", "cp", f"s3://{bucket}/{key}", "-"])
    except subprocess.CalledProcessError:
        return None
    try:
        value = json.loads(raw)
    except Exception:
        return None
    if not isinstance(value, dict):
        return None
    return value


def s3_put_json(bucket: str, key: str, payload: dict[str, Any]) -> None:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tf:
        tf.write(json.dumps(payload, separators=(",", ":")))
        tf.write("\n")
        temp_path = tf.name
    try:
        subprocess.check_call(
            [
                "aws",
                "s3api",
                "put-object",
                "--bucket",
                bucket,
                "--key",
                key,
                "--content-type",
                "application/json",
                "--body",
                temp_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=aws_env(),
        )
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass


def call_lock_script(
    *,
    action: str,
    bucket: str,
    key: str,
    owner: str,
    run_id: str,
    benchmark_uuid: str,
    lease_seconds: int,
    actor: str,
    acquire_timeout_seconds: int = 0,
    poll_seconds: float = 5.0,
) -> tuple[int, str]:
    cmd = [
        "python3",
        LOCK_SCRIPT,
        action,
        "--bucket",
        bucket,
        "--key",
        key,
        "--owner",
        owner,
        "--lease-seconds",
        str(lease_seconds),
        "--actor",
        actor,
    ]
    if action in {"acquire", "heartbeat"}:
        cmd += ["--run-id", run_id, "--benchmark-uuid", benchmark_uuid]
    if action == "acquire":
        cmd += [
            "--acquire-timeout-seconds",
            str(acquire_timeout_seconds),
            "--poll-seconds",
            str(poll_seconds),
        ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    output = (proc.stdout or "").strip() or (proc.stderr or "").strip()
    return proc.returncode, output


def parse_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def shard_counts(shards: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "queued": 0,
        "running": 0,
        "retrying": 0,
        "succeeded": 0,
        "failed": 0,
        "timed_out": 0,
        "total": len(shards),
    }
    for shard in shards:
        status = str(shard.get("status", "")).strip().lower()
        if status in counts:
            counts[status] += 1
    return counts


def maybe_instance_id() -> str:
    # Try IMDSv2 first for stable worker identity, then fallback to hostname.
    try:
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
        )
        with urllib.request.urlopen(token_req, timeout=1.0) as resp:
            token = resp.read().decode("utf-8").strip()
        if token:
            id_req = urllib.request.Request(
                "http://169.254.169.254/latest/meta-data/instance-id",
                headers={"X-aws-ec2-metadata-token": token},
            )
            with urllib.request.urlopen(id_req, timeout=1.0) as resp:
                instance_id = resp.read().decode("utf-8").strip()
            if instance_id:
                return instance_id
    except Exception:
        pass

    env_instance = os.environ.get("SCFUZZBENCH_INSTANCE_ID", "").strip()
    if env_instance:
        return env_instance

    return socket.gethostname().strip() or f"worker-{uuid.uuid4().hex[:8]}"


class QueueWorker:
    def __init__(self) -> None:
        self.bucket = require_env("SCFUZZBENCH_S3_BUCKET")
        self.run_id = require_env("SCFUZZBENCH_RUN_ID")
        self.benchmark_uuid = require_env("SCFUZZBENCH_BENCHMARK_UUID")
        self.lock_owner = require_env("SCFUZZBENCH_LOCK_OWNER")
        self.lock_key = os.environ.get("SCFUZZBENCH_LOCK_KEY", "").strip() or "runs/_control/global-lock.json"

        self.max_parallel_instances = env_int("SCFUZZBENCH_MAX_PARALLEL_INSTANCES", 1)
        self.shard_max_attempts = max(env_int("SCFUZZBENCH_SHARD_MAX_ATTEMPTS", 3), 1)
        self.lock_lease_seconds = max(env_int("SCFUZZBENCH_LOCK_LEASE_SECONDS", 900), 30)
        self.lock_heartbeat_seconds = max(env_int("SCFUZZBENCH_LOCK_HEARTBEAT_SECONDS", 60), 15)
        self.lock_acquire_timeout_seconds = max(env_int("SCFUZZBENCH_LOCK_ACQUIRE_TIMEOUT_SECONDS", 0), 0)
        self.poll_seconds = max(env_int("SCFUZZBENCH_QUEUE_POLL_SECONDS", 15), 5)
        self.idle_polls_before_exit = max(env_int("SCFUZZBENCH_QUEUE_IDLE_POLLS_BEFORE_EXIT", 6), 1)

        self.shards_json_b64 = require_env("SCFUZZBENCH_SHARDS_JSON_B64")

        self.worker_id = maybe_instance_id()
        self.hostname = socket.gethostname().strip()

        self.root_prefix = f"runs/{self.run_id}/{self.benchmark_uuid}"
        self.shard_prefix = f"{self.root_prefix}/queue/shards/"
        self.run_status_key = f"{self.root_prefix}/status/run.json"
        self.worker_status_key = f"{self.root_prefix}/status/workers/{sanitize(self.worker_id)}.json"
        self.event_prefix = f"{self.root_prefix}/status/events/"
        self.dlq_prefix = f"{self.root_prefix}/dlq/"

        self.stop_event = threading.Event()
        self.lock_lost_event = threading.Event()
        self.heartbeat_thread: threading.Thread | None = None
        self.last_run_state = ""

    def emit_event(self, *, shard_key: str, status: str, event_type: str, details: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {
            "event_at": iso_utc(),
            "event_type": event_type,
            "run_id": self.run_id,
            "benchmark_uuid": self.benchmark_uuid,
            "worker_id": self.worker_id,
            "shard_key": shard_key,
            "status": status,
        }
        if details:
            payload.update(details)
        key = (
            f"{self.event_prefix}{int(time.time() * 1000)}-"
            f"{sanitize(self.worker_id)}-{sanitize(shard_key)}-{sanitize(status)}-"
            f"{uuid.uuid4().hex[:8]}.json"
        )
        s3_put_json(self.bucket, key, payload)

    def update_worker_status(self, *, state: str, shard_key: str = "", attempt: int = 0, exit_code: int | None = None) -> None:
        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "benchmark_uuid": self.benchmark_uuid,
            "worker_id": self.worker_id,
            "hostname": self.hostname,
            "lock_owner": self.lock_owner,
            "state": state,
            "current_shard": shard_key,
            "attempt": attempt,
            "updated_at": iso_utc(),
        }
        if exit_code is not None:
            payload["last_exit_code"] = int(exit_code)
        s3_put_json(self.bucket, self.worker_status_key, payload)

    def load_shards(self) -> list[tuple[str, dict[str, Any]]]:
        out: list[tuple[str, dict[str, Any]]] = []
        for key in sorted(list_keys(self.bucket, self.shard_prefix)):
            if not key.endswith(".json"):
                continue
            shard = s3_json_or_none(self.bucket, key)
            if shard is None:
                continue
            out.append((key, shard))
        return out

    def refresh_run_status(self) -> dict[str, Any]:
        existing = s3_json_or_none(self.bucket, self.run_status_key) or {}
        shards = [item[1] for item in self.load_shards()]
        counts = shard_counts(shards)
        inflight = counts["queued"] + counts["running"] + counts["retrying"]
        terminal = inflight == 0 and counts["total"] > 0

        if terminal:
            state = "failed" if (counts["failed"] + counts["timed_out"]) > 0 else "succeeded"
        else:
            state = "running"

        payload: dict[str, Any] = {
            "mode": "s3_queue",
            "queue_mode": True,
            "run_id": self.run_id,
            "benchmark_uuid": self.benchmark_uuid,
            "state": state,
            "terminal": terminal,
            "counts": counts,
            "requested_shards": counts["total"],
            "max_parallel_instances": self.max_parallel_instances,
            "shard_max_attempts": self.shard_max_attempts,
            "lock_owner": self.lock_owner,
            "updated_at": iso_utc(),
        }

        created_at = str(existing.get("created_at", "")).strip()
        payload["created_at"] = created_at or payload["updated_at"]

        if terminal:
            completed_at = str(existing.get("completed_at", "")).strip()
            payload["completed_at"] = completed_at or payload["updated_at"]
        elif "completed_at" in existing and existing["completed_at"]:
            payload["completed_at"] = existing["completed_at"]

        s3_put_json(self.bucket, self.run_status_key, payload)

        if state != self.last_run_state:
            self.emit_event(
                shard_key="run",
                status=state,
                event_type="run_status",
                details={
                    "counts": counts,
                    "terminal": terminal,
                },
            )
            self.last_run_state = state

        return payload

    def heartbeat_loop(self) -> None:
        while not self.stop_event.wait(self.lock_heartbeat_seconds):
            rc, out = call_lock_script(
                action="heartbeat",
                bucket=self.bucket,
                key=self.lock_key,
                owner=self.lock_owner,
                run_id=self.run_id,
                benchmark_uuid=self.benchmark_uuid,
                lease_seconds=self.lock_lease_seconds,
                actor=self.worker_id,
            )
            if rc != 0:
                self.lock_lost_event.set()
                log(f"Lock heartbeat failed: {out}")
                return

    def acquire_lock(self) -> None:
        rc, out = call_lock_script(
            action="acquire",
            bucket=self.bucket,
            key=self.lock_key,
            owner=self.lock_owner,
            run_id=self.run_id,
            benchmark_uuid=self.benchmark_uuid,
            lease_seconds=self.lock_lease_seconds,
            acquire_timeout_seconds=self.lock_acquire_timeout_seconds,
            poll_seconds=5.0,
            actor=self.worker_id,
        )
        if rc != 0:
            raise RuntimeError(f"failed to acquire lock: {out}")
        log("Global S3 lock acquired")

    def release_lock(self) -> None:
        cmd = [
            "python3",
            LOCK_SCRIPT,
            "release",
            "--bucket",
            self.bucket,
            "--key",
            self.lock_key,
            "--owner",
            self.lock_owner,
        ]
        proc = subprocess.run(cmd, text=True, capture_output=True)
        if proc.returncode == 0:
            log("Released global S3 lock")
        else:
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            details = stdout or stderr or "unknown"
            log(f"Lock release skipped/failed: {details}")

    def initialize_queue(self) -> None:
        cmd = [
            "python3",
            QUEUE_INIT_SCRIPT,
            "--bucket",
            self.bucket,
            "--run-id",
            self.run_id,
            "--benchmark-uuid",
            self.benchmark_uuid,
            "--shards-json-b64",
            self.shards_json_b64,
            "--max-parallel-instances",
            str(self.max_parallel_instances),
            "--shard-max-attempts",
            str(self.shard_max_attempts),
            "--lock-owner",
            self.lock_owner,
        ]
        subprocess.check_call(cmd)

    def claim_shard(self) -> tuple[str, dict[str, Any]] | None:
        now_epoch = epoch_now()
        for key, shard in self.load_shards():
            status = str(shard.get("status", "")).strip().lower()
            if status not in {"queued", "retrying"}:
                continue

            retry_epoch = parse_int(shard.get("retry_available_at_epoch"), 0)
            if status == "retrying" and now_epoch < retry_epoch:
                continue

            attempt = parse_int(shard.get("attempt"), 0)
            max_attempts = max(parse_int(shard.get("max_attempts"), self.shard_max_attempts), 1)
            if attempt >= max_attempts:
                shard["status"] = "failed"
                shard["updated_at"] = iso_utc()
                shard["last_worker_id"] = self.worker_id
                shard["last_exit_code"] = parse_int(shard.get("last_exit_code"), 1)
                s3_put_json(self.bucket, key, shard)
                self.emit_event(
                    shard_key=str(shard.get("shard_key", key)),
                    status="failed",
                    event_type="shard_status",
                    details={"reason": "attempts_exhausted"},
                )
                continue

            claim_token = uuid.uuid4().hex
            shard["status"] = "running"
            shard["attempt"] = attempt + 1
            shard["claim_token"] = claim_token
            shard["updated_at"] = iso_utc()
            shard["started_at"] = shard["updated_at"]
            shard["last_worker_id"] = self.worker_id
            shard["retry_available_at_epoch"] = 0
            shard["retry_available_at"] = ""
            s3_put_json(self.bucket, key, shard)

            time.sleep(0.6)
            confirmed = s3_json_or_none(self.bucket, key)
            if not confirmed:
                continue
            if str(confirmed.get("claim_token", "")) != claim_token:
                continue
            if str(confirmed.get("status", "")).strip().lower() != "running":
                continue

            shard_key = str(confirmed.get("shard_key", "")).strip() or key.rsplit("/", 1)[-1].removesuffix(".json")
            self.emit_event(
                shard_key=shard_key,
                status="running",
                event_type="shard_status",
                details={
                    "attempt": parse_int(confirmed.get("attempt"), 1),
                },
            )
            return key, confirmed
        return None

    def run_shard(self, shard: dict[str, Any]) -> int:
        shard_key = str(shard.get("shard_key", "")).strip()
        fuzzer_key = str(shard.get("fuzzer_key", "")).strip()
        attempt = parse_int(shard.get("attempt"), 1)
        if not shard_key or not fuzzer_key:
            return 2

        script_path = f"/opt/scfuzzbench/fuzzers/{fuzzer_key}/run.sh"
        if not os.path.isfile(script_path):
            log(f"Missing run script for fuzzer '{fuzzer_key}'")
            return 127

        safe_shard = sanitize(shard_key)
        workdir = f"/opt/scfuzzbench/work/{safe_shard}/attempt-{attempt}"
        logdir = f"/opt/scfuzzbench/logs/{safe_shard}/attempt-{attempt}"

        shutil.rmtree(workdir, ignore_errors=True)
        shutil.rmtree(logdir, ignore_errors=True)
        os.makedirs(workdir, exist_ok=True)
        os.makedirs(logdir, exist_ok=True)

        env = os.environ.copy()
        env["SCFUZZBENCH_QUEUE_MODE"] = "1"
        env["SCFUZZBENCH_WORKDIR"] = workdir
        env["SCFUZZBENCH_LOG_DIR"] = logdir
        env["SCFUZZBENCH_SHARD_KEY"] = shard_key
        env["SCFUZZBENCH_SHARD_ATTEMPT"] = str(attempt)

        log(f"Running shard '{shard_key}' (fuzzer={fuzzer_key}, attempt={attempt})")
        proc = subprocess.run(["bash", script_path], env=env)
        return int(proc.returncode)

    def dlq(self, shard: dict[str, Any], exit_code: int, status: str) -> None:
        shard_key = str(shard.get("shard_key", "")).strip()
        attempt = parse_int(shard.get("attempt"), 1)
        key = f"{self.dlq_prefix}{sanitize(shard_key)}-{attempt}.json"
        payload = {
            "run_id": self.run_id,
            "benchmark_uuid": self.benchmark_uuid,
            "shard_key": shard_key,
            "status": status,
            "attempt": attempt,
            "max_attempts": parse_int(shard.get("max_attempts"), self.shard_max_attempts),
            "exit_code": int(exit_code),
            "worker_id": self.worker_id,
            "failed_at": iso_utc(),
        }
        s3_put_json(self.bucket, key, payload)

    def complete_shard(self, key: str, shard: dict[str, Any], exit_code: int) -> None:
        shard_key = str(shard.get("shard_key", "")).strip()
        attempt = parse_int(shard.get("attempt"), 1)
        max_attempts = max(parse_int(shard.get("max_attempts"), self.shard_max_attempts), 1)

        if exit_code == 0:
            terminal_status = "succeeded"
        elif exit_code == 124:
            terminal_status = "timed_out"
        else:
            terminal_status = "failed"

        now = utc_now()
        now_iso = iso_utc(now)

        shard["updated_at"] = now_iso
        shard["finished_at"] = now_iso
        shard["last_worker_id"] = self.worker_id
        shard["last_exit_code"] = int(exit_code)
        shard["claim_token"] = ""

        if terminal_status == "succeeded":
            shard["status"] = "succeeded"
            shard["retry_available_at_epoch"] = 0
            shard["retry_available_at"] = ""
            s3_put_json(self.bucket, key, shard)
            self.emit_event(
                shard_key=shard_key,
                status="succeeded",
                event_type="shard_status",
                details={"attempt": attempt, "exit_code": exit_code},
            )
            return

        if attempt < max_attempts:
            retry_seconds = min(300, (2 ** max(attempt - 1, 0)) * 30)
            retry_at = now + dt.timedelta(seconds=retry_seconds)
            shard["status"] = "retrying"
            shard["retry_available_at_epoch"] = int(retry_at.timestamp())
            shard["retry_available_at"] = iso_utc(retry_at)
            s3_put_json(self.bucket, key, shard)
            self.emit_event(
                shard_key=shard_key,
                status="retrying",
                event_type="shard_status",
                details={
                    "attempt": attempt,
                    "exit_code": exit_code,
                    "retry_in_seconds": retry_seconds,
                    "next_retry_at": shard["retry_available_at"],
                },
            )
            return

        shard["status"] = terminal_status
        shard["retry_available_at_epoch"] = 0
        shard["retry_available_at"] = ""
        s3_put_json(self.bucket, key, shard)
        self.emit_event(
            shard_key=shard_key,
            status=terminal_status,
            event_type="shard_status",
            details={"attempt": attempt, "exit_code": exit_code},
        )
        self.dlq(shard, exit_code=exit_code, status=terminal_status)

    def start_heartbeat(self) -> None:
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, name="lock-heartbeat", daemon=True)
        self.heartbeat_thread.start()

    def stop_heartbeat(self) -> None:
        self.stop_event.set()
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=5.0)

    def run(self) -> int:
        self.acquire_lock()
        self.start_heartbeat()

        try:
            self.initialize_queue()
            self.refresh_run_status()
            self.update_worker_status(state="idle")

            idle_polls = 0
            while True:
                if self.lock_lost_event.is_set():
                    raise RuntimeError("lock heartbeat failed; stopping worker")

                claimed = self.claim_shard()
                if claimed is None:
                    run_status = self.refresh_run_status()
                    if bool(run_status.get("terminal", False)):
                        log(f"Run reached terminal state: {run_status.get('state', '')}")
                        break

                    idle_polls += 1
                    self.update_worker_status(state="idle")
                    sleep_seconds = self.poll_seconds + random.randint(0, 3)
                    if idle_polls >= self.idle_polls_before_exit:
                        idle_polls = 0
                    time.sleep(sleep_seconds)
                    continue

                idle_polls = 0
                key, shard = claimed
                shard_key = str(shard.get("shard_key", "")).strip()
                attempt = parse_int(shard.get("attempt"), 1)
                self.update_worker_status(state="running", shard_key=shard_key, attempt=attempt)

                exit_code = self.run_shard(shard)
                self.update_worker_status(state="idle", shard_key=shard_key, attempt=attempt, exit_code=exit_code)
                self.complete_shard(key, shard, exit_code)
                run_status = self.refresh_run_status()
                if bool(run_status.get("terminal", False)):
                    log(f"Run reached terminal state: {run_status.get('state', '')}")
                    break

            final_status = s3_json_or_none(self.bucket, self.run_status_key) or {}
            self.update_worker_status(state="stopped")
            if bool(final_status.get("terminal", False)):
                self.release_lock()
            return 0

        finally:
            self.stop_heartbeat()


def main() -> int:
    try:
        worker = QueueWorker()
        return worker.run()
    except Exception as exc:
        log(f"Queue worker fatal error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
