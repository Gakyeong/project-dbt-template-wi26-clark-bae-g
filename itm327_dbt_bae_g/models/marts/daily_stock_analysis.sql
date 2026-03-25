{{ config(materialized='table') }}

with technical as (
    select *
    from {{ ref('mart_stock_technical') }}
),

risk as (
    select *
    from {{ ref('mart_stock_risk') }}
),

performance as (
    select *
    from {{ ref('mart_stock_performance') }}
),

news as (
    select *
    from {{ ref('mart_news_features') }}
),

merged as (
    select
        t.date_key,
        t.symbol,

        -- Prices & returns
        t.open_price,
        t.close_price,
        t.high_price,
        t.low_price,
        t.volume,
        t.daily_return,
        t.price_range,

        -- Technical indicators
        t.ma_5,
        t.ma_20,
        t.ma_50,
        t.macd_line,
        t.rsi_14,
        t.vol_20,

        -- Risk metrics
        r.beta_20,
        r.sharpe_20,
        r.drawdown,

        -- Performance metrics
        p.resistance_level,
        p.support_level,
        p.breakout_signal,
        p.trend_direction,
        p.rolling_return_20,
        p.cumulative_return,
        p.index_rolling_return_20,

        -- News features
        n.news_count,
        n.news_zscore_20,
        n.volume_zscore_20,
        n.news_vs_volume_zscore,
        n.vol_news_corr_20
    from technical t
    left join risk r
        on t.symbol = r.symbol
        and t.date_key = r.date_key
    left join performance p
        on t.symbol = p.symbol
        and t.date_key = p.date_key
    left join news n
        on t.symbol = n.symbol
        and t.date_key = n.date_key
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
)

select
    *,
    case
        when score >= 70 then 'BUY'
        when score >= 40 then 'HOLD'
        else 'SELL'
    end as recommendation
from scored
order by symbol, date_key;
