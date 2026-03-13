from snowflake.snowpark.functions import col, avg, stddev, max as max_, min as min_, count, sum as sum_, lit
from snowflake.snowpark.functions import when


def model(dbt, session):
    """
    Snowpark Python model: per-symbol volatility and volume metrics.

    Demonstrates dbt Python models using Snowpark DataFrames instead of SQL.
    Useful for complex calculations that are verbose in SQL, such as
    statistical measures (standard deviation) and conditional aggregations.

    Output: one row per stock symbol summarizing its full history in the dataset.
    """
    # Reference staging models via dbt.ref() — same as {{ ref() }} in SQL
    stocks_df = dbt.ref('raw_stocks')

    # Compute derived columns before aggregation
    stocks_with_metrics = stocks_df.withColumn(
        'DAILY_RANGE', col('HIGH') - col('LOW')
    ).withColumn(
        'DAILY_CHANGE', col('CLOSE') - col('OPEN')
    ).withColumn(
        'DAILY_RETURN_PCT', (col('CLOSE') - col('OPEN')) / col('OPEN') * lit(100)
    )

    # Aggregate per symbol across all trading days
    summary = (
        stocks_with_metrics
        .groupBy('SYMBOL')
        .agg(
            count('TRADE_DATE').alias('TRADING_DAYS'),
            min_('TRADE_DATE').alias('FIRST_TRADE_DATE'),
            max_('TRADE_DATE').alias('LAST_TRADE_DATE'),
            max_('HIGH').alias('PERIOD_HIGH'),
            min_('LOW').alias('PERIOD_LOW'),
            avg('CLOSE').alias('AVG_CLOSE'),
            avg('VOLUME').alias('AVG_DAILY_VOLUME'),
            sum_('VOLUME').alias('TOTAL_VOLUME'),
            avg('DAILY_RANGE').alias('AVG_DAILY_RANGE'),
            stddev('DAILY_RETURN_PCT').alias('RETURN_VOLATILITY'),
            avg('DAILY_RETURN_PCT').alias('AVG_DAILY_RETURN_PCT'),
            sum_(when(col('DAILY_CHANGE') > lit(0), lit(1)).otherwise(lit(0))).alias('UP_DAYS'),
            sum_(when(col('DAILY_CHANGE') < lit(0), lit(1)).otherwise(lit(0))).alias('DOWN_DAYS'),
        )
    )

    return summary
