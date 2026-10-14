from pathlib import Path
import pandas as pd

BRONZE_ROOT = Path("data/bronze")

parquet_files = sorted(
    BRONZE_ROOT.rglob("*.parquet")
)

if not parquet_files:
    raise FileNotFoundError(
        "No Bronze Parquet files found."
    )

for file_path in parquet_files:
    print("\n" + "=" * 80)
    print("FILE:", file_path)

    df = pd.read_parquet(file_path)

    print("\nROW COUNT:", len(df))

    print("\nSCHEMA:")
    print(df.dtypes.to_string())

    print("\nSAMPLE RECORDS:")
    print(df.head(3).to_string(index=False))

    print("\nNULL COUNTS:")
    print(df.isna().sum().to_string())