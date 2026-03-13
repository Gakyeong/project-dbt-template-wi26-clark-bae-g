-- TODO: Update the source table name to match your prefix (e.g., SMITHJ_WEATHER)
select *
from {{ source('snowbearair', 'LASTN_FI_WEATHER') }}
