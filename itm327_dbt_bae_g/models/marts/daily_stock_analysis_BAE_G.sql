{{ config(materialized='table') }}

-- 1. Base fact + dimension join
with base as (
    select
        f.date_key,
        f.stock_key,
        d.symbol,
        f.open_price,
        f.close_price,
        f.high_price,
        f.low_price,
        f.volume,
        (f.close_price - f.open_price) / f.open_price as daily_return,
        (f.high_price - f.low_price) as price_range
    from {{ ref('FACT_STOCK_BAE_G') }} f
    join {{ ref('DIM_STOCK_BAE_G') }} d
        on f.stock_key = d.stock_key
),

-- 2. Moving averages
ma as (
    select
        *,
        avg(close_price) over (
            partition by stock_key order by date_key
            rows between 4 preceding and current row
        ) as ma_5,

        avg(close_price) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        ) as ma_20,

        avg(close_price) over (
            partition by stock_key order by date_key
            rows between 49 preceding and current row
        ) as ma_50
    from base
),

-- 3. Volatility (20-day)
vol as (
    select
        *,
        stddev(close_price) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        ) as vol_20
    from ma
),

-- 4. MACD (12 vs 26 EMA approximated with windowed averages)
macd_calc as (
    select
        *,
        avg(close_price) over (
            partition by stock_key order by date_key
            rows between 11 preceding and current row
        ) as ema_12,

        avg(close_price) over (
            partition by stock_key order by date_key
            rows between 25 preceding and current row
        ) as ema_26
    from vol
),

macd as (
    select
        *,
        (ema_12 - ema_26) as macd_line
    from macd_calc
),

-- 5. RSI (14-day)
rsi_calc as (
    select
        *,
        lag(close_price) over (partition by stock_key order by date_key) as prev_close
    from macd
),

rsi_diff as (
    select
        *,
        case when close_price > prev_close then close_price - prev_close else 0 end as gain,
        case when close_price < prev_close then prev_close - close_price else 0 end as loss
    from rsi_calc
),

rsi as (
    select
        *,
        avg(gain) over (
            partition by stock_key order by date_key
            rows between 13 preceding and current row
        ) as avg_gain,

        avg(loss) over (
            partition by stock_key order by date_key
            rows between 13 preceding and current row
        ) as avg_loss
    from rsi_diff
),

rsi_final as (
    select
        *,
        case
            when avg_loss = 0 then 100
            else 100 - (100 / (1 + (avg_gain / avg_loss)))
        end as rsi_14
    from rsi
),

-- 6. Benchmark index (SPY)
index_returns as (
    select
        date_key,
        (close_price - open_price) / open_price as index_return
    from {{ ref('FACT_STOCK_BAE_G') }} f
    join {{ ref('DIM_STOCK_BAE_G') }} d
        on f.stock_key = d.stock_key
    where d.symbol = 'SPY'
),

-- 7. Beta (20-day rolling)
beta_calc as (
    select
        r.*,
        i.index_return,
        avg(r.daily_return) over (
            partition by r.stock_key order by r.date_key
            rows between 19 preceding and current row
        ) as avg_stock_ret,

        avg(i.index_return) over (
            partition by r.stock_key order by r.date_key
            rows between 19 preceding and current row
        ) as avg_index_ret
    from rsi_final r
    left join index_returns i
        on r.date_key = i.date_key
),

beta as (
    select
        *,
        covar_samp(daily_return, index_return) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        ) /
        var_samp(index_return) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        ) as beta_20
    from beta_calc
),

-- 8. Risk metrics
risk as (
    select
        *,
        avg(daily_return) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        ) /
        stddev(daily_return) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        ) as sharpe_20,

        close_price /
        max(close_price) over (
            partition by stock_key order by date_key
            rows between unbounded preceding and current row
        ) - 1 as drawdown
    from beta
),

-- 9. Price pattern detection
patterns as (
    select
        *,
        max(high_price) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        ) as resistance_level,

        min(low_price) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        ) as support_level,

        case when close_price >
            max(high_price) over (
                partition by stock_key order by date_key
                rows between 19 preceding and current row
            )
        then true else false end as breakout_signal,

        case
            when ma_5 > ma_20 then 'Uptrend'
            when ma_5 < ma_20 then 'Downtrend'
            else 'Sideways'
        end as trend_direction
    from risk
)

select *
from patterns
order by stock_key, date_key;
