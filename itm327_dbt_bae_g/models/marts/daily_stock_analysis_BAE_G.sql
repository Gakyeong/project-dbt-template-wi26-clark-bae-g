{{ config(materialized='table') }}

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

vol as (
    select
        *,
        stddev(daily_return) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        ) as vol_20
    from ma
),

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
            else 100 - (100 / (1 + (avg_gain / nullif(avg_loss, 0))))
        end as rsi_14
    from rsi
),

index_returns as (
    select
        f.date_key,
        (f.close_price - f.open_price) / f.open_price as index_return,
        f.close_price as index_close
    from {{ ref('FACT_STOCK_BAE_G') }} f
    join {{ ref('DIM_STOCK_BAE_G') }} d
        on f.stock_key = d.stock_key
    where d.symbol = 'SPY'
),

beta_calc as (
    select
        r.*,
        i.index_return,
        -- rolling sums for 20-day window
        sum(r.daily_return) over (
            partition by r.stock_key
            order by r.date_key
            rows between 19 preceding and current row
        ) as sum_stock_ret_20,
        sum(i.index_return) over (
            order by r.date_key
            rows between 19 preceding and current row
        ) as sum_index_ret_20,
        sum(r.daily_return * i.index_return) over (
            partition by r.stock_key
            order by r.date_key
            rows between 19 preceding and current row
        ) as sum_stock_index_ret_20,
        sum(i.index_return * i.index_return) over (
            order by r.date_key
            rows between 19 preceding and current row
        ) as sum_index_sq_20,
        count(i.index_return) over (
            order by r.date_key
            rows between 19 preceding and current row
        ) as n_obs_20
    from rsi_final r
    left join index_returns i
        on r.date_key = i.date_key
),

beta as (
    select
        *,
        case
            when n_obs_20 >= 2
                 and (sum_index_sq_20 - (sum_index_ret_20 * sum_index_ret_20) / n_obs_20) <> 0
            then
                (
                    sum_stock_index_ret_20
                    - (sum_stock_ret_20 * sum_index_ret_20) / n_obs_20
                )
                /
                (
                    sum_index_sq_20
                    - (sum_index_ret_20 * sum_index_ret_20) / n_obs_20
                )
            else null
        end as beta_20
    from beta_calc
),

risk as (
    select
        *,
        avg(daily_return) over (
            partition by stock_key order by date_key
            rows between 19 preceding and current row
        )
        /
        nullif(
            stddev(daily_return) over (
                partition by stock_key order by date_key
                rows between 19 preceding and current row
            ),
            0
        ) as sharpe_20,
        close_price /
        max(close_price) over (
            partition by stock_key order by date_key
            rows between unbounded preceding and current row
        ) - 1 as drawdown
    from beta
),

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
),

perf as (
    select
        p.*,
        -- 20-day rolling return
        (p.close_price /
         lag(p.close_price, 20) over (partition by p.stock_key order by p.date_key)
        ) - 1 as rolling_return_20,
        -- cumulative return from first date
        (p.close_price /
         first_value(p.close_price) over (partition by p.stock_key order by p.date_key)
        ) - 1 as cumulative_return,
        -- index rolling return (20-day)
        (