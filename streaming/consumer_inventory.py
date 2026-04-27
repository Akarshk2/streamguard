"""
StreamGuard — Inventory Topic Consumer
PySpark Structured Streaming consumer for inventory_topic.
"""

import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("streamguard.consumer.inventory")

spark = (
    SparkSession.builder
    .appName("StreamGuard-Inventory")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

INVENTORY_SCHEMA = StructType([
    StructField("sku",            StringType(),  True),
    StructField("warehouse_id",   StringType(),  True),
    StructField("stock_level",    IntegerType(), True),
    StructField("reserved_qty",   IntegerType(), True),
    StructField("restock_flag",   BooleanType(), True),
    StructField("restock_qty",    IntegerType(), True),
    StructField("unit_cost",      DoubleType(),  True),
    StructField("last_movement",  StringType(),  True),
    StructField("event_time",     StringType(),  True),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    .option("subscribe", "inventory_topic")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), INVENTORY_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_topic", F.lit("inventory_topic"))
)

sys.path.insert(0, os.path.dirname(__file__))
from schema_validator import validate_and_route
from stream_profiler import generate_profile
from snowflake_writer import write_to_snowflake


def process_batch(batch_df, batch_id: int):
    count = batch_df.count()
    if count == 0:
        return

    logger.info(f"Batch {batch_id}: received {count} inventory records")

    good_df, bad_df = validate_and_route(batch_df, "inventory", batch_id)
    write_to_snowflake(good_df, "RAW.INVENTORY_RAW")
    generate_profile(batch_df, "inventory_topic", batch_id)

    logger.info(f"Batch {batch_id} complete")


(
    parsed.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/tmp/streamguard/checkpoints/inventory")
    .start()
    .awaitTermination()
)
