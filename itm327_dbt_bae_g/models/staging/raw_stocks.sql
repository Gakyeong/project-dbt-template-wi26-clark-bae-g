-- TODO: Update the source table name to match your prefix (e.g., SMITHJ_STOCKS)
select *
from {{ source('snowbearair', 'LASTN_FI_STOCKS') }}
