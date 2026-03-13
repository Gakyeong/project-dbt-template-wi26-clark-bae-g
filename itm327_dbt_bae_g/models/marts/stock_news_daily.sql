-- stock_news_daily
-- Mart 1: Daily stock performance enriched with news coverage.
-- Reads from fct_daily_stocks (warehouse layer) — no raw joins here.
-- Each row = one stock symbol on one trading day.

SELECT
    f.SYMBOL,
    f.TRADE_DATE,
    f.OPEN,
    f.HIGH,
    f.LOW,
    f.CLOSE,
    f.VOLUME,
    f.DAILY_RANGE,
    f.DAILY_CHANGE,
    f.PCT_CHANGE,
    f.DIRECTION,
    f.NEWS_ARTICLE_COUNT,
    f.NEWS_CATEGORIES,
    f.HEADLINES,

    -- Enrich with symbol-level context from the dimension
    d.ALL_TIME_HIGH,
    d.ALL_TIME_LOW,
    d.AVG_CLOSE_PRICE,

    -- How far is today's close from the symbol's all-time high?
    ROUND((f.CLOSE - d.ALL_TIME_HIGH) / NULLIF(d.ALL_TIME_HIGH, 0) * 100, 4) AS PCT_FROM_ATH

FROM {{ ref('fct_daily_stocks') }} f
LEFT JOIN {{ ref('dim_symbols') }} d
    ON f.SYMBOL = d.SYMBOL
