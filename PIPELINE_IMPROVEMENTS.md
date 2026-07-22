# Climate Data Lakehouse - Pipeline Improvements

## What Was Changed

### Before: Fake Data Generator
- Generated deterministic but fictional climate data (5 days, 4 Canadian cities)
- Stored in CSV format in Bronze layer
- No versioning or incremental loading capability

### After: Real Climate Data from Open-Meteo API
- Fetches **real historical and current climate data** from [Open-Meteo](https://open-meteo.com/) (free, no API key required)
- Implements **incremental loading** with watermark tracking
- Stores raw data in **Parquet format** (more efficient, columnar, supports append-only patterns)
- Automatically deduplicates records when re-running
- Production-ready error handling and logging

## Key Features

### 1. **Incremental Ingestion with Watermarks**
- **First run**: Backfills 5 years of historical data
- **Subsequent runs**: Only fetches last 2 days + lookback buffer to catch updates
- Watermark saved to `.watermark.json` to track progress per location

```python
# Example: Run on day 1 and day 5
# Day 1: Fetches 2020-01-01 to 2025-01-01 (5 years)
# Day 5: Fetches 2024-12-30 to 2025-01-05 (2-day lookback + new data)
```

### 2. **Append-Only Bronze Layer**
- All raw data stored in `data/bronze/climate_raw.parquet`
- New data is appended to existing data on each run
- Duplicates are automatically removed (last occurrence wins)
- Full audit trail available in Git if stored with DVC/Delta Lake

### 3. **Open-Meteo API Integration**
Fetches these metrics for each location:
- **Temperature**: Daily mean temperature (°C)
- **Precipitation**: Daily precipitation sum (mm)
- **Metadata**: Latitude, longitude, location name

Locations tracked:
- Toronto: 43.6532°N, 79.3832°W
- Ottawa: 45.4215°N, 75.6972°W
- Montreal: 45.5019°N, 73.5674°W
- Vancouver: 49.2827°N, 123.1207°W

### 4. **Improved Data Pipeline**
```
Open-Meteo API
    ↓
Bronze (Parquet) - Raw data, append-only
    ↓
Transform - Type enforcement, deduplication
    ↓
Silver (Parquet) - Cleaned, typed data
    ↓
Load - Postgres staging table
    ↓
Gold (dbt) - Fact tables, dimensions (via dbt)
```

## Pipeline Execution

```bash
# Run entire pipeline (Prefect orchestration)
python src/orchestration/flow.py

# Or run individual steps:
python src/ingest/ingest.py        # Fetch from API, write to Bronze
python src/transform/transform.py  # Bronze → Silver
python src/load/load_postgres.py   # Silver → Postgres
```

## Configuration

Update these in `ingest_incremental()` call:
- `lookback_days`: Days to refetch in incremental runs (default: 2)
- `backfill_years`: Years to fetch on first run (default: 5)

```python
# In ingest.py main()
ingest_incremental(data_dir, lookback_days=2, backfill_years=5)
```

## Files Modified

1. **`src/ingest/ingest.py`**
   - Replaced `generate_sample_climate_csv()` with `fetch_climate_data()` and `ingest_incremental()`
   - Added watermark tracking (`_load_watermark()`, `_save_watermark()`)
   - API error handling and logging

2. **`src/transform/transform.py`**
   - Updated `main()` to support both Parquet and CSV inputs
   - Maintains backwards compatibility with old CSV format

## Dependencies

All required packages are already in `requirements.txt`:
- `requests` - HTTP calls to Open-Meteo API
- `pandas` - Data manipulation
- `pyarrow` - Parquet format support
- `python-dotenv` - Environment variables

## Next Steps (Optional Enhancements)

1. **Add more locations** by updating `LOCATIONS` list
2. **Add more metrics** by updating `daily` parameter in API call:
   ```python
   "daily": "temperature_2m_mean,precipitation_sum,windspeed_10m_max,cloud_cover_mean"
   ```
3. **Store watermarks in database** instead of JSON file
4. **Add data quality checks** (nulls, outliers, freshness)
5. **Implement Delta Lake** for better ACID transactions
6. **Add unit tests** for `fetch_climate_data()` and watermark logic

## Example Output

```
INFO - Fetching data for Toronto from 2020-01-01 to 2025-01-01
INFO - Successfully fetched 1827 records for Toronto
INFO - Fetching data for Ottawa from 2020-01-01 to 2025-01-01
INFO - Successfully fetched 1827 records for Ottawa
...
INFO - ✓ Ingestion complete: 7308 records in Bronze
INFO - ✓ Watermark saved: {'LOC001': '2025-01-01', 'LOC002': '2025-01-01', ...}
```

## Data Quality Notes

- Open-Meteo API quality: High-quality reanalysis data (validated against weather stations)
- Missing data: Rare; handled gracefully with logging
- Data latency: 2-3 days behind real-time
- Temperature range: Expected -40°C to +40°C for Canadian locations
- Precipitation: Can be 0 on dry days

## Troubleshooting

**Error: "No daily data for Toronto"**
- Check internet connectivity
- Verify API endpoint: https://archive-api.open-meteo.com/v1/archive
- Check latitude/longitude values

**Watermark not updating**
- Ensure `data/bronze/.watermark.json` is writable
- Check file permissions

**Duplicate records in Silver**
- This is expected on re-runs; duplicates are automatically removed during transform
- Check `transform.py` line with `drop_duplicates()`
