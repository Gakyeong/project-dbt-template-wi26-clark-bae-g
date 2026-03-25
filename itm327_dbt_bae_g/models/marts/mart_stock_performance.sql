{{ config(materialized='table') }}

with patterns as (
    select
        *,
        max(high_price) over (partition by symbol order by date_key rows between 19 preceding and current row) as resistance_level,
        min(low_price) over (partition by symbol order by date_key rows between 19 preceding and current row) as support_level,
        case when close_price >
            max(high_price) over (partition by symbol order by date_key rows between 19 preceding and current row)
        then true else false end as breakout_signal,
        case
            when ma_5 > ma_20 then 'Uptrend'
            when ma_5 < ma_20 then 'Downtrend'
            else 'Sideways'
        end as trend_direction
    from {{ ref('mart_stock_technical') }}
),

index_returns as (
    select
        f.date_key,
        f.close_price as index_close
    from {{ ref('FACT_STOCK_BAE_G') }} f
    join {{ ref('DIM_STOCK_BAE_G') }} d
        on f.stock_key = d.stock_key
    where d.symbol = 'SPY'
),

select
    p.*,
    (p.close_price /
     lag(p.close_price, 20) over (partition by p.symbol order by p.date_key)
    ) - 1 as rolling_return_20,

    (p.close_price /
     first_value(p.close_price) over (partition by p.symbol order by p.date_key)
    ) - 1 as cumulative_return,

    (i.index_close /
     lag(i.index_close, 20) over (order by p.date_key)
    ) - 1 as index_rolling_return_20
from patterns p
left join index_returns i
    on p.date_key = i.date_key
order by symbol, date_key;
