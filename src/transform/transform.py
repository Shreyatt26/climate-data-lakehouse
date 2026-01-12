from __future__ import annotations

from pathlib import Path
import os

import pandas as pd
from dotenv import load_dotenv


def _get_data_dir() -> Path:
    load_dotenv()
    return Path(os.getenv("DATA_DIR", "./data"))


def csv_to_parquet(in_csv: Path, out_parquet: Path) -> Path:
    """
    Read Bronze CSV and write Silver Parquet with basic type enforcement.
    """
    df = pd.read_csv(in_csv)

    ## Convert bronze csv data types to parquet friendly data types
    
    # Type enforcement (helps later when loading to a DB)
    df["date"] = pd.to_datetime(df["date"])
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)
    df["t_mean_c"] = df["t_mean_c"].astype(float)
    df["precip_mm"] = df["precip_mm"].astype(float)

    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_parquet, index=False)
    return out_parquet


def main() -> None:
    data_dir = _get_data_dir()
    bronze_csv = data_dir / "bronze" / "sample_climate_daily.csv"
    silver_parquet = data_dir / "silver" / "climate_daily.parquet"

    if not bronze_csv.exists():
        raise FileNotFoundError(
            f"Bronze CSV not found at {bronze_csv}. Run: python src/ingest/ingest.py"
        )

    out_path = csv_to_parquet(bronze_csv, silver_parquet)
    print(f"[OK] Wrote Silver Parquet: {out_path} ({out_path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
