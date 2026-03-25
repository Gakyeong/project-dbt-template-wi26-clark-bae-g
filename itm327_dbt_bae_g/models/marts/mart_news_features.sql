{{ config(materialized='table') }}

with news_base as (
    select
        to_number(to_char(datetime, 'YYYYMMDD')) as date_key,
        related as symbol,
        count(*) as news_count
    from {{ ref('DIM_NEWS_BAE_G') }}
    where related is not null
    group by 1, 2
),

news_stats as (
    select
        *,
        avg(news_count) over (partition by symbol order by date_key rows between 20 preceding and current row) as avg_news_20,
        stddev(news_count) over (partition by symbol order by date_key rows between 20 preceding and current row) as std_news_20
    from news_base
),

news_z as (
    select
        *,
        (news_count - avg_news_20) / nullif(std_news_20, 0) as news_zscore_20
    from news_stats
),

volume_stats as (
    select
        f.date_key,
        d.symbol,
        f.volume,
        avg(f.volume) over (partition by d.symbol order by f.date_key rows between 20 preceding and current row) as avg_volume_20,
        stddev(f.volume) over (partition by d.symbol order by f.date_key rows between 20 preceding and current row) as std_volume_20
    from {{ ref('FACT_STOCK_BAE_G') }} f
    join {{ ref('DIM_STOCK_BAE_G') }} d
        on f.stock_key = d.stock_key
),

volume_z as (
    select
        *,
        (volume - avg_volume_20) / nullif(std_volume_20, 0) as volume_zscore_20
    from volume_stats
)

select
    n.date_key,
    n.symbol,
    n.news_count,
    n.news_zscore_20,
    v.volume_zscore_20,
    (n.news_zscore_20 - v.volume_zscore_20) as news_vs_volume_zscore,
    corr(n.news_count, v.volume) over (
        partition by n.symbol order by n.date_key rows between 20 preceding and current row
    ) as vol_news_corr_20
from news_z n
left join volume_z v
    on n.date_key = v.date_key
    and n.symbol = v.symbol
order by symbol, date_key;
