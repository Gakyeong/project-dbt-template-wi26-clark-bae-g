{{ config(materialized='table') }}

with base as (
    select
        f.date_key,
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

ma as (
    select
        *,
        avg(close_price) over (partition by symbol order by date_key rows between 4 preceding and current row) as ma_5,
        avg(close_price) over (partition by symbol order by date_key rows between 19 preceding and current row) as ma_20,
        avg(close_price) over (partition by symbol order by date_key rows between 49 preceding and current row) as ma_50
    from base
),

vol as (
    select
        *,
        stddev(daily_return) over (partition by symbol order by date_key rows between 19 preceding and current row) as vol_20
    from ma
),

macd_calc as (
    select
        *,
        avg(close_price) over (partition by symbol order by date_key rows between 11 preceding and current row) as ema_12,
        avg(close_price) over (partition by symbol order by date_key rows between 25 preceding and current row) as ema_26
    from vol
),

macd as (
    select
        *,
        (ema_12 - ema_26) as macd_line
    from macd_calc
),

rsi_calc as (
    select
        *,
        lag(close_price) over (partition by symbol order by date_key) as prev_close
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
        avg(gain) over (partition by symbol order by date_key rows between 13 preceding and current row) as avg_gain,
        avg(loss) over (partition by symbol order by date_key rows between 13 preceding and current row) as avg_loss
    from rsi_diff
),

select
    *,
    case when avg_loss = 0 then 100
         else 100 - (100 / (1 + (avg_gain / nullif(avg_loss,0))))
    end as rsi_14
from rsi
order by symbol, date_key;
