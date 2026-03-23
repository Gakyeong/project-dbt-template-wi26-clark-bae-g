{{ config(materialized='table') }}

select
    dense_rank() over (order by symbol) as stock_key, -- integer, ordering, 
    symbol
from {{ ref('raw_stocks') }}
group by symbol
