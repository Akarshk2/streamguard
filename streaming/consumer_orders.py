"""
StreamGuard — Orders Topic Consumer
PySpark Structured Streaming consumer for the orders_topic Kafka topic.
"""

import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("streamguard.consumer.orders")

spark = (
    SparkSession.builder
    .appName("StreamGuard-Orders")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/streamguard/checkpoints/orders")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

ORDER_SCHEMA = StructType([
    StructField("order_id",       StringType(),  True),
    StructField("customer_id",    StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("amount",         DoubleType(),  True),
    StructField("currency",       StringType(),  True),
    StructField("status",         StringType(),  True),
    StructField("payment_method", StringType(),  True),
    StructField("region",         StringType(),  True),
    StructField("event_time",     StringType(),  True),
    StructField("created_at",     StringType(),  True),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    .option("subscribe", "orders_topic")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 5000)
    .load()
)

parsed = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), ORDER_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_topic", F.lit("orders_topic"))
    .withColumn("ingest_date", F.to_date(F.current_timestamp()))
)

sys.path.insert(0, os.path.dirname(__file__))
from schema_validator import validate_and_route
from stream_profiler import generate_profile
from snowflake_writer import write_to_snowflake


def process_batch(batch_df, batch_id: int):
    count = batch_df.count()
    if count == 0:
        logger.info(f"Batch {batch_id}: empty — skipping")
        return

    logger.info(f"Batch {batch_id}: received {count} order records")

    # 1. Validate and route good/bad records
    good_df, bad_df = validate_and_route(batch_df, "order", batch_id)

    # 2. Write good records to Snowflake RAW layer
    write_to_snowflake(good_df, "RAW.ORDERS_RAW")

    # 3. Generate and write profile directly via Python connector
    generate_profile(batch_df, "orders_topic", batch_id)

    logger.info(f"Batch {batch_id} complete")


query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/tmp/streamguard/checkpoints/orders")
    .start()
)

logger.info("Orders consumer started. Awaiting termination...")
query.awaitTermination()
