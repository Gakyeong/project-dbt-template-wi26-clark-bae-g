-- TODO: Update the source table name to match your prefix (e.g., SMITHJ_NEWS)
select *
from {{ source('snowbearair', 'LASTN_FI_NEWS') }}
