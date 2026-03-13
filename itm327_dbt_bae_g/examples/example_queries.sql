-- =============================================================
-- ITM327 dbt Project — Example Queries
-- Run these in Snowflake after dbt run completes.
-- Replace LASTN_FI with your actual prefix everywhere.
-- =============================================================

-- -------------------------------------------------------
-- 1. What raw tables exist in your RAW schema?
-- -------------------------------------------------------
SHOW TABLES IN SCHEMA SNOWBEARAIR_DB.RAW;

-- -------------------------------------------------------
-- 2. Preview each raw table
-- -------------------------------------------------------
SELECT * FROM SNOWBEARAIR_DB.RAW.LASTN_FI_STOCKS    LIMIT 10;
SELECT * FROM SNOWBEARAIR_DB.RAW.LASTN_FI_NEWS      LIMIT 10;
SELECT * FROM SNOWBEARAIR_DB.RAW.LASTN_FI_WEATHER   LIMIT 10;

-- -------------------------------------------------------
-- 3. How much data did your Airflow pipelines load?
-- -------------------------------------------------------
SELECT COUNT(*) FROM SNOWBEARAIR_DB.RAW.LASTN_FI_STOCKS;
SELECT COUNT(*) FROM SNOWBEARAIR_DB.RAW.LASTN_FI_NEWS;
SELECT COUNT(*) FROM SNOWBEARAIR_DB.RAW.LASTN_FI_WEATHER;

-- -------------------------------------------------------
-- 4. Mart 1: stock_news_daily
--    What stocks had the most news coverage on a given day?
-- -------------------------------------------------------
SELECT
    SYMBOL,
    TRADE_DATE,
    CLOSE,
    ROUND(PCT_CHANGE, 2)   AS PCT_CHANGE,
    NEWS_ARTICLE_COUNT,
    HEADLINES
FROM SNOWBEARAIR_DB.DEV.STOCK_NEWS_DAILY
WHERE TRADE_DATE = '2024-11-01'
ORDER BY NEWS_ARTICLE_COUNT DESC
LIMIT 20;

-- -------------------------------------------------------
-- 5. Mart 1: Did high news volume correlate with big price moves?
-- -------------------------------------------------------
SELECT
    SYMBOL,
    AVG(NEWS_ARTICLE_COUNT) AS AVG_NEWS_COUNT,
    AVG(ABS(PCT_CHANGE))    AS AVG_ABS_PCT_CHANGE
FROM SNOWBEARAIR_DB.DEV.STOCK_NEWS_DAILY
GROUP BY SYMBOL
ORDER BY AVG_ABS_PCT_CHANGE DESC
LIMIT 20;

-- -------------------------------------------------------
-- 6. Mart 2: weather_market_conditions
--    How did the market perform on rainy vs clear days in New York?
-- -------------------------------------------------------
SELECT
    CITY,
    CASE
        WHEN PRECIP > 5  THEN 'Rainy (>5mm)'
        WHEN PRECIP > 0  THEN 'Light Rain (0-5mm)'
        ELSE 'Dry'
    END                              AS WEATHER_TYPE,
    COUNT(*)                         AS TRADING_DAYS,
    ROUND(AVG(AVG_PCT_CHANGE), 4)   AS AVG_MARKET_RETURN_PCT,
    ROUND(AVG(STOCKS_UP), 0)        AS AVG_STOCKS_UP,
    ROUND(AVG(STOCKS_DOWN), 0)      AS AVG_STOCKS_DOWN
FROM SNOWBEARAIR_DB.DEV.WEATHER_MARKET_CONDITIONS
WHERE CITY = 'New York'
GROUP BY CITY, WEATHER_TYPE
ORDER BY WEATHER_TYPE;

-- -------------------------------------------------------
-- 7. Mart 3 (Python/Snowpark): market_volatility_metrics
--    Which stocks were the most volatile over your data window?
-- -------------------------------------------------------
SELECT
    SYMBOL,
    TRADING_DAYS,
    PERIOD_HIGH,
    PERIOD_LOW,
    ROUND(AVG_CLOSE, 2)             AS AVG_CLOSE,
    ROUND(RETURN_VOLATILITY, 4)     AS RETURN_VOLATILITY,
    ROUND(AVG_DAILY_RETURN_PCT, 4)  AS AVG_DAILY_RETURN_PCT,
    UP_DAYS,
    DOWN_DAYS
FROM SNOWBEARAIR_DB.DEV.MARKET_VOLATILITY_METRICS
ORDER BY RETURN_VOLATILITY DESC
LIMIT 20;

-- -------------------------------------------------------
-- 8. Understand a mart query before running it in dbt
--    (The underlying SQL behind stock_news_daily)
-- -------------------------------------------------------
SELECT
    s.SYMBOL,
    s.TRADE_DATE,
    s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME,
    s.HIGH - s.LOW                                              AS DAILY_RANGE,
    ROUND((s.CLOSE - s.OPEN) / NULLIF(s.OPEN, 0) * 100, 4)   AS PCT_CHANGE,
    COUNT(n.ID)                                                AS NEWS_ARTICLE_COUNT
FROM SNOWBEARAIR_DB.RAW.LASTN_FI_STOCKS s
LEFT JOIN SNOWBEARAIR_DB.RAW.LASTN_FI_NEWS n
    ON  s.SYMBOL     = n.RELATED
    AND s.TRADE_DATE = n.DATETIME::DATE
GROUP BY s.SYMBOL, s.TRADE_DATE, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME
ORDER BY NEWS_ARTICLE_COUNT DESC
LIMIT 20;
