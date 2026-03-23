select
    "date" as raw_date,
    "city",
    "max_temp",
    "min_temp",
    "precip",
    "max_wind",
    "appt_max",
    "appt_min",
    "appt_temp_range",
    "temp_range_c",
    "daylight_duration",
    "sunrise",
    "sunset"
from {{ source('snowbearair', 'WEATHER_API_BAE_G') }}
