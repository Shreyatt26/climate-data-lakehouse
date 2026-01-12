from __future__ import annotations

from pathlib import Path
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def _get_env() -> tuple[Path, str]:
    load_dotenv()
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        raise ValueError("DATABASE_URL not set. Add it to your .env file.")
    return data_dir, db_url

def main() -> None:
    data_dir, db_url = _get_env()

    parquet_path = data_dir / "silver" / "climate_daily.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Silver Parquet not found at {parquet_path}. "
            "Run: python src/ingest/ingest.py then python src/transform/transform.py"
        )
    
    df = pd.read_parquet(parquet_path)

    engine = create_engine(db_url)

    with engine.begin() as conn:
        df.to_sql("stg_climate_daily", con=conn, if_exists="replace", index=False)
        row_count = conn.execute(text("SELECT COUNT(*) FROM stg_climate_daily")).scalar_one()

    print(f"[OK] Loaded {row_count} rows into Postgres table: stg_climate_daily")


if __name__ == "__main__":
    main()