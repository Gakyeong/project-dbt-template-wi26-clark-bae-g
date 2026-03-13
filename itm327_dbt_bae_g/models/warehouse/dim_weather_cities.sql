-- dim_weather_cities
-- Dimension table: one row per financial hub city tracked by the weather pipeline.
--
-- In your data warehouse, this acts as the "location" dimension —
-- marts that analyze market performance by geography join against this table.
--
-- TODO: Add a city-to-exchange mapping if you want to associate each city with
--       a specific stock exchange (e.g., New York → NYSE/NASDAQ, London → LSE).

SELECT
    CITY,

    -- Date range this city appears in your weather data
    MIN(DATE)   AS FIRST_OBSERVATION_DATE,
    MAX(DATE)   AS LAST_OBSERVATION_DATE,
    COUNT(*)    AS TOTAL_OBSERVATION_DAYS,

    -- Typical conditions (descriptive stats across the full history)
    ROUND(AVG(MAX_TEMP), 2)   AS AVG_MAX_TEMP,
    ROUND(AVG(MIN_TEMP), 2)   AS AVG_MIN_TEMP,
    ROUND(AVG(PRECIP), 2)     AS AVG_DAILY_PRECIP,
    ROUND(AVG(MAX_WIND), 2)   AS AVG_MAX_WIND,

    -- Extreme conditions
    MAX(MAX_TEMP)             AS HOTTEST_DAY_TEMP,
    MIN(MIN_TEMP)             AS COLDEST_DAY_TEMP,
    MAX(PRECIP)               AS WETTEST_DAY_PRECIP

FROM {{ ref('raw_weather') }}
GROUP BY CITY
