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
            else 100 - (100 / (1 + (avg_gain / nullif(avg_loss,0))))
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
        i.index_return
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
        )
        /
        nullif(
            var_samp(index_return) over (
                order by date_key
                rows between 19 preceding and current row
            ),
            0
        ) as beta_20
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
        (p.close_price /
         lag(p.close_price, 20) over (partition by p.stock_key order by p.date_key)
        ) - 1 as rolling_return_20,

        (p.close_price /
         first_value(p.close_price) over (partition by p.stock_key order by p.date_key)
        ) - 1 as cumulative_return,

        (i.index_close /
         lag(i.index_close, 20) over (order by p.date_key)
        ) - 1 as index_rolling_return_20
    from patterns p
    left join index_returns i
        on p.date_key = i.date_key
),

news_base as (
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
        avg(news_count) over (
            partition by symbol order by date_key
            rows between 20 preceding and current row
        ) as avg_news_20,
        stddev(news_count) over (
            partition by symbol order by date_key
            rows between 20 preceding and current row
        ) as std_news_20
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
        avg(f.volume) over (
            partition by d.symbol order by f.date_key
            rows between 20 preceding and current row
        ) as avg_volume_20,
        stddev(f.volume) over (
            partition by d.symbol order by f.date_key
            rows between 20 preceding and current row
        ) as std_volume_20
    from {{ ref('FACT_STOCK_BAE_G') }} f
    join {{ ref('DIM_STOCK_BAE_G') }} d
        on f.stock_key = d.stock_key
),

volume_z as (
    select
        *,
        (volume - avg_volume_20) / nullif(std_volume_20, 0) as volume_zscore_20
    from volume_stats
),

news_joined as (
    select
        n.date_key,
        n.symbol,
        n.news_count,
        n.news_zscore_20,
        v.volume_zscore_20,
        (n.news_zscore_20 - v.volume_zscore_20) as news_vs_volume_zscore,
        corr(n.news_count, v.volume) over (
            partition by n.symbol order by n.date_key
            rows between 20 preceding and current row
        ) as vol_news_corr_20
    from news_z n
    left join volume_z v
        on n.date_key = v.date_key
        and n.symbol = v.symbol
),


merged as (
    select
        p.*,
        n.news_count,
        n.news_zscore_20,
        n.volume_zscore_20,
        n.news_vs_volume_zscore,
        n.vol_news_corr_20
    from perf p
    left join news_joined n
        on p.date_key = n.date_key
        and p.symbol = n.symbol
),


scored as (
    select
        *,
        0
        + case when trend_direction = 'Uptrend' then 10 else 0 end
        + case when close_price > ma_50 then 10 else 0 end
        + case when macd_line > 0 then 10 else 0 end
        + case when rsi_14 between 50 and 65 then 10 else 0 end
        + case when breakout_signal then 10 else 0 end
        + case when rolling_return_20 > index_rolling_return_20 then 10 else 0 end
        + case when sharpe_20 > 1 then 10 else 0 end
        + case when drawdown > -0.10 then 10 else 0 end
        + case when beta_20 between 0 and 1.5 then 10 else 0 end
        + case when news_zscore_20 > 1 then 5 else 0 end
        + case when volume_zscore_20 > 1 then 5 else 0 end
        as score
    from merged
),


final as (
    select
        *,
        case
            when score >= 70 then 'BUY'
            when score >= 40 then 'HOLD'
            else 'SELL'
        end as recommendation
    from scored
)

select *
from final
order by stock_key, date_key;
