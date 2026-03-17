-- TODO: Update the source table name to match your prefix (e.g., SMITHJ_STOCKS)
select *
from {{ source('snowbearair', 'STOCK_API_BAE_G') }}
