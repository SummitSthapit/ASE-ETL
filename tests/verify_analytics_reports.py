
"""Verify the newest analytics CSV export."""

import csv
import hashlib
import json
from pathlib import Path

from src.analytics.reporting import OUTPUT_ROOT, REPORTS

def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for block in iter(
            lambda: file.read(1024 * 1024), b""
        ):
            digest.update(block)

    return digest.hexdigest()


def main():
    runs = sorted(
        path for path in OUTPUT_ROOT.iterdir()
        if path.is_dir()
        and not path.name.startswith(".staging_")
    )

    if not runs:
        raise RuntimeError("No report export directories found.")

    latest = runs[-1]

    manifest = json.loads(
        (latest / "manifest.json").read_text(
            encoding="utf-8"
        )
    )

    assert len(manifest["reports"]) == len(REPORTS)

    for report_name, source_view in REPORTS.items():
        metadata = manifest["reports"][report_name]
        path = latest / metadata["file"]

        assert path.is_file()
        assert metadata["source_view"] == source_view
        assert sha256_file(path) == metadata["sha256"]

        with path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as file:
            reader = csv.reader(file)

            assert next(reader) == metadata["columns"]

            row_count = sum(1 for _ in reader)

        assert row_count == metadata["rows"]

        print(
            f"PASS: {report_name} "
            f"— {row_count:,} verified rows"
        )

    print("\nPASS: Analytics report integrity verified")


if __name__ == "__main__":
    main()
