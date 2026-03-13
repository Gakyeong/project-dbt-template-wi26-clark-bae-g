-- weather_market_conditions
-- Mart 2: Daily weather at financial hub cities alongside broad market performance.
-- Reads from warehouse layer only (fct_daily_weather, fct_daily_stocks, dim_weather_cities).
-- Each row = one city on one day.
-- Use this mart to explore whether weather at financial hubs correlates with market moves.
--
-- NOTE: This mart references fct_daily_weather (warehouse layer), not raw_weather (staging).
-- Best practice: marts should always build off warehouse-layer models, never staging directly.

SELECT
    w.DATE                                              AS TRADE_DATE,
    w.CITY,

    -- Weather conditions
    w.MAX_TEMP,
    w.MIN_TEMP,
    w.PRECIP,
    w.MAX_WIND,

    -- City context from the dimension
    c.AVG_MAX_TEMP                                      AS CITY_NORMAL_MAX_TEMP,
    w.MAX_TEMP - c.AVG_MAX_TEMP                        AS TEMP_DEVIATION_FROM_NORMAL,

    -- Broad market performance for that day
    COUNT(DISTINCT f.SYMBOL)                           AS STOCKS_TRACKED,
    ROUND(AVG(f.PCT_CHANGE), 4)                        AS AVG_PCT_CHANGE,
    ROUND(AVG(f.DAILY_RANGE), 4)                       AS AVG_DAILY_RANGE,
    SUM(f.VOLUME)                                      AS TOTAL_MARKET_VOLUME,
    SUM(CASE WHEN f.DIRECTION = 'UP'   THEN 1 ELSE 0 END) AS STOCKS_UP,
    SUM(CASE WHEN f.DIRECTION = 'DOWN' THEN 1 ELSE 0 END) AS STOCKS_DOWN,
    SUM(CASE WHEN f.DIRECTION = 'FLAT' THEN 1 ELSE 0 END) AS STOCKS_FLAT

FROM {{ ref('fct_daily_weather') }} w
LEFT JOIN {{ ref('dim_weather_cities') }} c
    ON w.CITY = c.CITY
LEFT JOIN {{ ref('fct_daily_stocks') }} f
    ON w.DATE = f.TRADE_DATE
GROUP BY
    w.DATE,
    w.CITY,
    w.MAX_TEMP,
    w.MIN_TEMP,
    w.PRECIP,
    w.MAX_WIND,
    c.AVG_MAX_TEMP
