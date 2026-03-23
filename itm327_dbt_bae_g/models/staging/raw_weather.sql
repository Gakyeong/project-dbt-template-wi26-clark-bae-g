-- TODO: Update the source table name to match your prefix (e.g., SMITHJ_WEATHER)
select
    date as raw_date,
    city,
    appt_max,
    appt_min,
    appt_temp_range,
    max_temp,
    min_temp,
    precip,
    max_wind,
    sunrise,
    sunset,
    daylight_duration,
    temp_range_c
from {{ source('snowbearair', 'WEATHER_API_BAE_G') }}