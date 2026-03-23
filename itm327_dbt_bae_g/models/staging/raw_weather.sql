-- TODO: Update the source table name to match your prefix (e.g., SMITHJ_WEATHER)
select
    "DATE" as raw_date,
    CITY,
    MAX_TEMP,
    MIN_TEMP,
    PRECIP,
    MAX_WIND,
    APPT_MAX,
    APPT_MIN,
    APPT_TEMP_RANGE,
    TEMP_RANGE_C,
    DAYLIGHT_DURATION,
    SUNRISE,
    SUNSET
from {{ source('snowbearair', 'WEATHER_API_BAE_G') }}