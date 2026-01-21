select date, location_id, avg(t_mean_c) as avg_temp_c, sum(precip_mm) as total_precip_mm from {{ ref('stg_climate_daily') }}
group by 1, 2