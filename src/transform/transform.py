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
    # Try new parquet bronze format first, fall back to CSV for backwards compatibility
    bronze_parquet = data_dir / "bronze" / "climate_raw.parquet"
    bronze_csv = data_dir / "bronze" / "sample_climate_daily.csv"
    silver_parquet = data_dir / "silver" / "climate_daily.parquet"

    if bronze_parquet.exists():
        # New path: Parquet to Parquet with transformations
        df = pd.read_parquet(bronze_parquet)
        df["date"] = pd.to_datetime(df["date"])
        df["lat"] = df["lat"].astype(float)
        df["lon"] = df["lon"].astype(float)
        df["t_mean_c"] = df["t_mean_c"].astype(float)
        df["precip_mm"] = df["precip_mm"].astype(float)
        
        silver_parquet.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(silver_parquet, index=False)
        print(f"[OK] Wrote Silver Parquet from Bronze: {silver_parquet} ({silver_parquet.stat().st_size} bytes)")
    elif bronze_csv.exists():
        # Legacy path: CSV to Parquet
        out_path = csv_to_parquet(bronze_csv, silver_parquet)
        print(f"[OK] Wrote Silver Parquet: {out_path} ({out_path.stat().st_size} bytes)")
    else:
        raise FileNotFoundError(
            f"Bronze data not found at {bronze_parquet} or {bronze_csv}. "
            "Run: python src/ingest/ingest.py"
        )


if __name__ == "__main__":
    main()
