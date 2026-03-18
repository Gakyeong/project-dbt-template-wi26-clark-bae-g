{{ config(materialized='table') }}
with src as (
    select
        id,
        datetime
    from {{ source('snowbearair', 'NEWS_API_BAE_G') }}
    where datetime is not null
),

dim as (
    select
        news_id,
        news_key
    from {{ ref('DIM_NEWS_BAE_G') }}
)

select
    to_number(to_char(s.datetime, 'YYYYMMDD')) as date_key,
    d.news_key
from src s
join dim d
    on d.news_id = s.id
