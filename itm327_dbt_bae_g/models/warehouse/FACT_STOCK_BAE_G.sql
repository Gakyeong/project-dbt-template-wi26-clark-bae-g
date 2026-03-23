{{ config(
    materialized='incremental',
    unique_key=['stock_key', 'date_key']
) }}

with src as (
    select
        to_number(to_char(datetime, 'YYYYMMDD')) as date_key,
        symbol,
        open as open_price,
        high as high_price,
        low as low_price,
        close as close_price,
        volume
    from {{ source('raw_stocks') }}
    where datetime is not null
),

dim_stock as (
    select
        stock_key,
        symbol
    from {{ ref('DIM_STOCK_BAE_G') }}
),

prepared as (
    select
        s.date_key,
        d.stock_key,
        s.open_price,
        s.high_price,
        s.low_price,
        s.close_price,
        s.volume
    from src s
    join dim_stock d
        on s.symbol = d.symbol
)

select *
from prepared

{% if is_incremental() %}
where date_key > (select max(date_key) from {{ this }})
{% endif %}
