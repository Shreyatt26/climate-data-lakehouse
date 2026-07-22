"""
Ingest real climate data from Open-Meteo API into Bronze layer (append-only).
Supports incremental loading with watermark tracking.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import json
import os
import logging

import pandas as pd
import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Open-Meteo locations to track
LOCATIONS = [
    {"location_id": "LOC001", "name": "Toronto", "lat": 43.6532, "lon": -79.3832},
    {"location_id": "LOC002", "name": "Ottawa", "lat": 45.4215, "lon": -75.6972},
    {"location_id": "LOC003", "name": "Montreal", "lat": 45.5019, "lon": -73.5674},
    {"location_id": "LOC004", "name": "Vancouver", "lat": 49.2827, "lon": -123.1207}
]

OPEN_METEO_API = "https://archive-api.open-meteo.com/v1/archive"


def _get_data_dir() -> Path:
    load_dotenv()
    return Path(os.getenv("DATA_DIR", "./data"))


def _get_watermark_file(data_dir: Path) -> Path:
    """Return path to watermark file that tracks last ingestion date"""
    return data_dir / "bronze" / ".watermark.json"


def _load_watermark(watermark_path: Path) -> dict:
    """Load watermark tracking last ingestion date per location"""
    if watermark_path.exists():
        with open(watermark_path) as f:
            return json.load(f)
    return {}


def _save_watermark(watermark_path: Path, watermark: dict) -> None:
    """Save watermark to track ingestion progress"""
    watermark_path.parent.mkdir(parents=True, exist_ok=True)
    with open(watermark_path, "w") as f:
        json.dump(watermark, f)


def fetch_climate_data(
    location: dict,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Fetch climate data from Open-Meteo API for a single location.
    
    Args:
        location: Dict with location_id, name, lat, lon
        start_date: Start date for historical data
        end_date: End date for historical data
        
    Returns:
        DataFrame with daily climate metrics
    """
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_mean,precipitation_sum",
        "timezone": "UTC",
    }
    
    logger.info(f"Fetching data for {location['name']} from {start_date} to {end_date}")
    
    try:
        response = requests.get(OPEN_METEO_API, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if "daily" not in data:
            logger.warning(f"No daily data for {location['name']}")
            return pd.DataFrame()
        
        daily = data["daily"]
        df = pd.DataFrame({
            "date": pd.to_datetime(daily["time"]),
            "location_id": location["location_id"],
            "location_name": location["name"],
            "lat": location["lat"],
            "lon": location["lon"],
            "t_mean_c": daily["temperature_2m_mean"],
            "precip_mm": daily["precipitation_sum"],
            "data_source": "open_meteo_api"
        })
        
        logger.info(f"Successfully fetched {len(df)} records for {location['name']}")
        return df
        
    except requests.RequestException as e:
        logger.error(f"API request failed for {location['name']}: {e}")
        return pd.DataFrame()


def ingest_incremental(
    data_dir: Path,
    lookback_days: int = 2,
    backfill_years: int = 5,
) -> None:
    """
    Incrementally ingest climate data.
    
    On first run: Backfill historical data (lookback_years)
    On subsequent runs: Only fetch data since last watermark
    
    Args:
        data_dir: Path to data directory
        lookback_days: Days back to fetch in incremental runs
        backfill_years: Years back to fetch on first run
    """
    bronze_dir = data_dir / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    watermark_file = _get_watermark_file(data_dir)
    watermark = _load_watermark(watermark_file)
    
    all_dfs = []
    
    for location in LOCATIONS:
        loc_id = location["location_id"]
        
        # Determine date range
        if loc_id in watermark:
            # Incremental: fetch from last watermark + lookback buffer
            last_date = datetime.fromisoformat(watermark[loc_id]).date()
            start_date = last_date - timedelta(days=lookback_days)
        else:
            # First run: backfill historical data
            start_date = date.today() - timedelta(days=365 * backfill_years)
        
        end_date = date.today()
        
        df = fetch_climate_data(location, start_date, end_date)
        if not df.empty:
            all_dfs.append(df)
            watermark[loc_id] = end_date.isoformat()
    
    if not all_dfs:
        logger.warning("No data fetched from API")
        return
    
    # Combine all locations
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Append to bronze parquet (append-only pattern)
    bronze_parquet = bronze_dir / "climate_raw.parquet"
    
    if bronze_parquet.exists():
        # Read existing data and append new data, removing duplicates
        existing_df = pd.read_parquet(bronze_parquet)
        combined_df = pd.concat([existing_df, combined_df], ignore_index=True)
        # Remove duplicates (keep last occurrence for updates)
        combined_df = combined_df.drop_duplicates(
            subset=["date", "location_id"],
            keep="last"
        )
        logger.info(f"Appending to existing Bronze data ({len(combined_df)} total records)")
    else:
        logger.info(f"Creating new Bronze Parquet ({len(combined_df)} records)")
    
    combined_df = combined_df.sort_values(["location_id", "date"]).reset_index(drop=True)
    combined_df.to_parquet(bronze_parquet, index=False)
    
    # Save watermark
    _save_watermark(watermark_file, watermark)
    
    logger.info(f"✓ Ingestion complete: {len(combined_df)} records in Bronze")
    logger.info(f"✓ Watermark saved: {watermark}")


def main() -> None:
    data_dir = _get_data_dir()
    ingest_incremental(data_dir, lookback_days=2, backfill_years=5)


if __name__ == "__main__":
    main()