select
    "date"              as raw_date,
    "city"              as city,
    "max_temp"          as max_temp,
    "min_temp"          as min_temp,
    "precip"            as precip,
    "max_wind"          as max_wind,
    "appt_max"          as appt_max,
    "appt_min"          as appt_min,
    "appt_temp_range"   as appt_temp_range,
    "temp_range_c"      as temp_range_c,
    "daylight_duration" as daylight_duration,
    "sunrise"           as sunrise,
    "sunset"            as sunset
from {{ source('snowbearair', 'WEATHER_API_BAE_G') }}
