{{ config(materialized='table') }}
select
    -- surrogate key for each article: not sequential, greate for scd2 or fact table, with natural key
    {{ dbt_utils.generate_surrogate_key(['id']) }} as news_key,

    -- raw fields
    id as news_id,
    category,
    headline,
    summary,
    url,
    image,
    source,
    related,

    -- derived keys
    to_number(to_char(datetime, 'YYYYMMDD')) as date_key,
    -- to_number(to_char(datetime, 'HH24MI'))   as time_key

from {{ source('snowbearair', 'NEWS_API_BAE_G') }}
where datetime is not null