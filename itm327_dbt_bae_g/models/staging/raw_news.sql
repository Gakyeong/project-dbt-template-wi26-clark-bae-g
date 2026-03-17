-- TODO: Update the source table name to match your prefix (e.g., SMITHJ_NEWS)
select *
from {{ source('snowbearair', 'NEWS_API_BAE_G') }}
