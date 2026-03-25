{{ config(materialized='table') }}

with index_returns as (
    select
        f.date_key,
        f.close_price as index_close,
        (f.close_price - f.open_price) / f.open_price as index_return
    from {{ ref('FACT_STOCK_BAE_G') }} f
    join {{ ref('DIM_STOCK_BAE_G') }} d
        on f.stock_key = d.stock_key
    where d.symbol = 'SPY'
),

joined as (
    select
        t.*,
        i.index_return
    from {{ ref('mart_stock_technical') }} t
    left join index_returns i
        on t.date_key = i.date_key
),

beta as (
    select
        *,
        covar_samp(daily_return, index_return) over (
            partition by symbol order by date_key rows between 19 preceding and current row
        )
        /
        nullif(
            var_samp(index_return) over (order by date_key rows between 19 preceding and current row),
            0
        ) as beta_20
    from joined
)

select
    *,
    avg(daily_return) over (partition by symbol order by date_key rows between 19 preceding and current row)
    /
    nullif(stddev(daily_return) over (partition by symbol order by date_key rows between 19 preceding and current row), 0)
    as sharpe_20,

    close_price /
    max(close_price) over (partition by symbol order by date_key rows between unbounded preceding and current row)
    - 1 as drawdown
from beta
order by symbol, date_key;
