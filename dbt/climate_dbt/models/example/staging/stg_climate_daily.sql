select cast(date as date) as date, location_id, location_name, lat, lon, t_mean_c, precip_mm, data_source from {{ ref('stg_climate_daily')}}
