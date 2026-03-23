{{ config(materialized='table') }}

select
    row_number() over (order by id) as news_key,

    id as news_id,
    category,
    headline,
    summary,
    url,
    image,
    source,
    related,

    to_number(to_char(datetime, 'YYYYMMDD')) as date_key

from {{ source('snowbearair', 'NEWS_API_BAE_G') }}
where datetime is not null
