#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile


def aws_env(profile: str | None) -> dict:
    env = os.environ.copy()
    if profile:
        env["AWS_PROFILE"] = profile
    return env


def list_keys(bucket: str, prefix: str, profile: str | None) -> list[str]:
    cmd = [
        "aws",
        "s3api",
        "list-objects-v2",
        "--bucket",
        bucket,
        "--prefix",
        prefix,
        "--query",
        "Contents[].Key",
        "--output",
        "json",
    ]
    output = subprocess.check_output(cmd, env=aws_env(profile))
    if not output:
        return []
    return json.loads(output)


def download_zip(bucket: str, key: str, dest_zip: Path, profile: str | None) -> None:
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(
        ["aws", "s3", "cp", f"s3://{bucket}/{key}", str(dest_zip), "--no-progress"],
        env=aws_env(profile),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Download and unzip scfuzzbench run artifacts from S3.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--benchmark-uuid",
        default=None,
        help="Benchmark UUID used in S3 prefixes (logs/<uuid>/<run_id>/...).",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Override the S3 prefix (useful for custom layouts).",
    )
    parser.add_argument("--dest", required=True, type=Path)
    parser.add_argument("--profile", default=None)
    parser.add_argument(
        "--category",
        choices=("logs", "corpus", "both"),
        default="logs",
        help="Artifact category to download.",
    )
    parser.add_argument("--no-unzip", action="store_true")
    args = parser.parse_args()

    categories = ["logs", "corpus"] if args.category == "both" else [args.category]
    total = 0
    for category in categories:
        if args.prefix:
            prefix = args.prefix.rstrip("/") + "/"
        elif args.benchmark_uuid:
            prefix = f"{category}/{args.benchmark_uuid}/{args.run_id}/"
        else:
            prefix = f"{category}/{args.run_id}/"
        keys = list_keys(args.bucket, prefix, args.profile)
        if not keys:
            print(f"No {category} artifacts found under {prefix}.")
            continue
        zip_dir = args.dest / category / "zips"
        unzip_dir = args.dest / category / "unzipped"
        for key in keys:
            name = os.path.basename(key)
            dest_zip = zip_dir / name
            dest_unzip = unzip_dir / os.path.splitext(name)[0]
            download_zip(args.bucket, key, dest_zip, args.profile)
            if not args.no_unzip:
                if not name.endswith(".zip"):
                    # Skip non-zip artifacts (e.g., manifest.json).
                    continue
                dest_unzip.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(dest_zip) as archive:
                    archive.extractall(dest_unzip)
            total += 1
        print(f"Downloaded {len(keys)} {category} zip(s) to {zip_dir}")
        if args.no_unzip:
            continue
    if total == 0:
        print("No artifacts downloaded.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
