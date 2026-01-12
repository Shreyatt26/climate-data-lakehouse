## Creates a small CSV

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import os

import pandas as pd
from dotenv import load_dotenv

def _get_data_dir() -> Path:
    load_dotenv()
    return Path(os.getenv("DATA_DIR", "./data"))

def generate_sample_climate_csv(out_path: Path, start: date = date(2010,1,1), days: int = 5,) -> Path:
    """
    Create a small, realistic climate-style dataset (daily metrics for 3 locations) and write
    it to Bronze as CSV
    """
    locations = [
        {"location_id": "LOC001", "name": "Toronto", "lat": 43.6532, "lon": -79.3832},
        {"location_id": "LOC002", "name": "Ottawa", "lat": 45.4215, "lon": -75.6972},
        {"location_id": "LOC003", "name": "Montreal", "lat": 45.5019, "lon": -73.5674},
        {"location_id": "LOC004", "name": "Vancouver", "lat": 49.2827, "lon": -123.1207}
    ]

    rows = []

    for i in range(days):
        d = start + timedelta(days=i)
        for idx, loc in enumerate(locations):
            ## Deterministic (non-random) values so the dataset is reproducible
            t_mean_c = 2.0 + 0.6 * i + idx * 0.8
            precip_mm = max(0.0, (i % 5) * 1.2 - idx * 0.3)

            rows.append(
                {
                    "date": d.isoformat(),
                    "location_id": loc["location_id"],
                    "location_name": loc["name"],
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "t_mean_c": round(t_mean_c,2),
                    "precip_mm": round(precip_mm, 2),
                    "data_source": "sample_generator"
                }
            )
    df = pd.DataFrame(rows)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def main() -> None:
    data_dir = _get_data_dir()
    bronze_dir = data_dir / "bronze"
    out_csv = bronze_dir / "sample_climate_daily.csv"

    path = generate_sample_climate_csv(out_csv)
    print(f"[OK] Wrote Bronze CSV: {path} ({path.stat().st_size} bytes)")

if __name__ == "__main__":
    main()