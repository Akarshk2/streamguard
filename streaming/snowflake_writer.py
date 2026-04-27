"""
StreamGuard — Snowflake Writer
Handles writing PySpark DataFrames to Snowflake using the Spark-Snowflake connector.
"""

import os
import logging
from dotenv import load_dotenv
from pyspark.sql import DataFrame

load_dotenv("C:/streamguard/.env")

logger = logging.getLogger("streamguard.snowflake_writer")

SNOWFLAKE_OPTIONS = {
    "sfURL":      f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser":     os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfWarehouse":os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfRole":     os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
}


def write_to_snowflake(df: DataFrame, table: str, mode: str = "append") -> bool:
    """
    Write a PySpark DataFrame to a Snowflake table.
    Uses coalesce(1) to avoid Python worker crash on Windows.
    """
    if df is None:
        return False
    try:
        df.coalesce(1).write \
            .format("net.snowflake.spark.snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("dbtable", table) \
            .mode(mode) \
            .save()
        logger.info(f"Written to Snowflake table: {table}")
        return True
    except Exception as e:
        logger.error(f"Failed to write to {table}: {e}")
        return False


def execute_snowflake_sql(sql: str) -> list:
    """Execute arbitrary SQL on Snowflake via the Python connector."""
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()
