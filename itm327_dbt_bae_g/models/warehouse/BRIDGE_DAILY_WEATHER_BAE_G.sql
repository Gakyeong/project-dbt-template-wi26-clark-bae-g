{{ config(materialized='table') }}

with src as (
    select
        to_number(to_char(to_date(raw_date), 'YYYYMMDD')) as date_key,
        city
    from {{ ref('raw_weather') }}
    where raw_date is not null
),

dim as (
    select
        weather_key,
        city,
        date_key
    from {{ ref('DIM_WEATHER_BAE_G') }}
)

select
    d.date_key,
    d.weather_key
from dim d
join src s
    on d.city = s.city
   and d.date_key = s.date_key
