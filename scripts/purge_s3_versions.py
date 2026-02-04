#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def aws_env(profile: str | None) -> dict:
    env = os.environ.copy()
    if profile:
        env["AWS_PROFILE"] = profile
    return env


def list_object_versions(bucket: str, prefix: str | None, profile: str | None) -> list[dict]:
    cmd = ["aws", "s3api", "list-object-versions", "--bucket", bucket, "--output", "json"]
    if prefix:
        cmd.extend(["--prefix", prefix])
    output = subprocess.check_output(cmd, env=aws_env(profile))
    data = json.loads(output or b"{}")
    objects: list[dict] = []
    for entry in data.get("Versions", []):
        objects.append({"Key": entry["Key"], "VersionId": entry["VersionId"]})
    for entry in data.get("DeleteMarkers", []):
        objects.append({"Key": entry["Key"], "VersionId": entry["VersionId"]})
    return objects


def delete_chunk(bucket: str, objects: list[dict], profile: str | None) -> None:
    payload = {"Objects": objects, "Quiet": True}
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as handle:
        json.dump(payload, handle)
        path = handle.name
    try:
        subprocess.check_call(
            ["aws", "s3api", "delete-objects", "--bucket", bucket, "--delete", f"file://{path}"],
            env=aws_env(profile),
        )
    finally:
        Path(path).unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Delete all object versions in a bucket.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--prefix", default=None)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()

    objects = list_object_versions(args.bucket, args.prefix, args.profile)
    if not objects:
        print("No object versions to delete.")
        return 0

    total = len(objects)
    for idx in range(0, total, args.batch_size):
        chunk = objects[idx : idx + args.batch_size]
        delete_chunk(args.bucket, chunk, args.profile)
        print(f"Deleted {min(idx + args.batch_size, total)}/{total} objects")
    return 0


if __name__ == "__main__":
    sys.exit(main())
