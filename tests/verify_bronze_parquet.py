
from pathlib import Path
import pandas as pd

root = Path("data/bronze/parquet")

if not root.exists():
    raise FileNotFoundError(f"Parquet directory not found: {root}")

total_rows = 0

for folder in sorted(root.iterdir()):
    if not folder.is_dir():
        continue

    parquet_files = sorted(folder.glob("*.parquet"))

    if not parquet_files:
        print(f"{folder.name}: NO PARQUET FILES")
        continue

    row_count = sum(
        len(pd.read_parquet(file))
        for file in parquet_files
    )

    total_rows += row_count

    print(
        f"{folder.name}: {row_count:,} rows "
        f"across {len(parquet_files)} Parquet part(s)"
    )

print("-" * 60)
print(f"TOTAL: {total_rows:,} rows")
