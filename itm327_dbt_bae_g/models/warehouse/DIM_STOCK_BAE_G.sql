{{ config(materialized='table') }}

select
    dense_rank() over (order by symbol) as stock_key, -- integer, ordering, 
    symbol
from {{ source('snowbearair', 'STOCK_API_BAE_G') }}
group by symbol
