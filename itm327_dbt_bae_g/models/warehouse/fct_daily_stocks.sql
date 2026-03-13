-- fct_daily_stocks
-- Fact table: one row per stock symbol per trading day.
-- Grain: SYMBOL + TRADE_DATE
--
-- This is the central fact table for your data warehouse.
-- It references dimension tables (dim_symbols, dim_weather_cities)
-- and adds calculated metrics that marts can aggregate from.
--
-- TODO: Review columns and adjust any casting or renaming to match
--       your raw data. Run `dbt run --select fct_daily_stocks` to build.

SELECT
    -- Keys (link to dimension tables)
    s.SYMBOL,
    s.TRADE_DATE,

    -- Price metrics (from raw OHLCV)
    s.OPEN,
    s.HIGH,
    s.LOW,
    s.CLOSE,
    s.VOLUME,

    -- Derived price metrics (calculated here so marts don't repeat logic)
    s.HIGH - s.LOW                                              AS DAILY_RANGE,
    s.CLOSE - s.OPEN                                           AS DAILY_CHANGE,
    ROUND((s.CLOSE - s.OPEN) / NULLIF(s.OPEN, 0) * 100, 4)   AS PCT_CHANGE,

    -- Direction flag (useful for counting up/down days in marts)
    CASE
        WHEN s.CLOSE > s.OPEN THEN 'UP'
        WHEN s.CLOSE < s.OPEN THEN 'DOWN'
        ELSE 'FLAT'
    END AS DIRECTION,

    -- News coverage for this symbol on this day
    COUNT(n.ID)                                                AS NEWS_ARTICLE_COUNT,
    ARRAY_AGG(n.HEADLINE)                                      AS HEADLINES,
    ARRAY_AGG(DISTINCT n.CATEGORY)                             AS NEWS_CATEGORIES

FROM {{ ref('raw_stocks') }} s
LEFT JOIN {{ ref('raw_news') }} n
    ON  s.SYMBOL     = n.RELATED
    AND s.TRADE_DATE = n.DATETIME::DATE

GROUP BY
    s.SYMBOL,
    s.TRADE_DATE,
    s.OPEN,
    s.HIGH,
    s.LOW,
    s.CLOSE,
    s.VOLUME
