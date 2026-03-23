{{ config(
    materialized='incremental',
    unique_key=['city', 'date_key']
) }}

with src as (
    select
        to_number(to_char(to_date(date), 'YYYYMMDD')) as date_key,
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
    from {{ ref('raw_weather') }}
    where date is not null
)

select
    sha2(city || '-' || date_key, 256) as weather_key,
    *
from src

{% if is_incremental() %}
  where date_key > (select max(date_key) from {{ this }})
{% endif %}
