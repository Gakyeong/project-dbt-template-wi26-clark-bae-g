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
    from {{ source('snowbearair', 'WEATHER_API_BAE_G') }}
    where date is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'date_key']) }} as weather_key,
    * --no natual key, having two with surrogate_key, incremental
from src

{% if is_incremental() %}
  where date_key > (select max(date_key) from {{ this }})
{% endif %}
