"""Pin exact source bytes for local and MinIO ingestion run recovery."""

import hashlib
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio

from src.orchestration.config import PROJECT_ROOT, STATE_ROOT


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_filenames(filenames):
    if not filenames:
        raise RuntimeError("No source CSV files found")
    for name in filenames:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*\.csv", name, re.IGNORECASE):
            raise ValueError(f"Unsupported source CSV filename: {name!r}")
    table_names = [Path(name).stem.lower().replace("-", "_") + "_raw" for name in filenames]
    if len(table_names) != len(set(table_names)):
        raise ValueError("Multiple CSV files map to the same Bronze table")


def local_source_snapshot():
    source_dir = PROJECT_ROOT / "data" / "raw"
    files = sorted(p for p in source_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv") if source_dir.is_dir() else []
    _validate_filenames([p.name for p in files])
    return {"source": "local", "files": {p.name: sha256_file(p) for p in files}}


def minio_source_snapshot():
    # Match Bronze ingestion's endpoint, credentials, and source prefix.
    load_dotenv(PROJECT_ROOT / ".env", override=True)
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    bucket = os.getenv("SOURCE_BUCKET", "ase-source")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "ase-bronze")
    prefix_name = os.getenv("SOURCE_PREFIX", "raw/").strip("/")
    prefix = f"{prefix_name}/" if prefix_name else ""
    if bucket == bronze_bucket:
        raise ValueError("SOURCE_BUCKET and BRONZE_BUCKET must differ")
    client = Minio(endpoint, access_key=os.environ["MINIO_ROOT_USER"],
                   secret_key=os.environ["MINIO_ROOT_PASSWORD"], secure=secure)
    if not client.bucket_exists(bucket):
        raise FileNotFoundError(f"MinIO source bucket does not exist: {bucket}")

    entries = []
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        key = obj.object_name
        if not key.startswith(prefix):
            continue
        name = key[len(prefix):]
        if "/" not in name and name.lower().endswith(".csv"):
            entries.append((name, key))
    entries.sort()
    _validate_filenames([name for name, _ in entries])

    hashes = {}
    for name, key in entries:
        response = client.get_object(bucket, key)
        digest = hashlib.sha256()
        try:
            for chunk in response.stream(1024 * 1024):
                digest.update(chunk)
        finally:
            response.close()
            response.release_conn()
        hashes[name] = digest.hexdigest()

    # Snapshot has the same logical filename/hash structure as local input;
    # the source and object namespace prevent ambiguous recovery.
    return {"source": "minio", "bucket": bucket, "prefix": prefix,
            "files": hashes}


def current_source_snapshot(source):
    if source == "local":
        return local_source_snapshot()
    if source == "minio":
        return minio_source_snapshot()
    raise ValueError(f"Unsupported source mode: {source!r}")


def verify_or_pin_snapshot(run_id, source):
    current = current_source_snapshot(source)
    snapshot_path = STATE_ROOT / f"source_{run_id}_{source}.json"
    if snapshot_path.exists():
        previous = json.loads(snapshot_path.read_text(encoding="utf-8"))
        if previous != current:
            raise RuntimeError("Source files changed since this run started; use a NEW run ID")
        print("PASS: Original source snapshot unchanged", flush=True)
        return
    with snapshot_path.open("x", encoding="utf-8") as stream:
        json.dump(current, stream, indent=2, sort_keys=True)
    print("PASS: Source snapshot pinned", flush=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Read-only source inventory and SHA-256 verification")
    parser.add_argument("--source", choices=["local", "minio"], required=True)
    args = parser.parse_args()
    snapshot = current_source_snapshot(args.source)
    print(f"Source: {snapshot['source']}")
    if args.source == "minio":
        print(f"Bucket: {snapshot['bucket']}; prefix: {snapshot['prefix']}")
    print(f"CSV files: {len(snapshot['files'])}")
    for name, digest in snapshot["files"].items():
        print(f"  {name}: {digest}")
