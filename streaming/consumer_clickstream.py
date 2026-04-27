"""
StreamGuard — Clickstream Topic Consumer
PySpark Structured Streaming consumer for clickstream_topic.
"""

import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("streamguard.consumer.clickstream")

spark = (
    SparkSession.builder
    .appName("StreamGuard-Clickstream")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

CLICK_SCHEMA = StructType([
    StructField("session_id",   StringType(),  True),
    StructField("user_id",      StringType(),  True),
    StructField("page",         StringType(),  True),
    StructField("action",       StringType(),  True),
    StructField("device",       StringType(),  True),
    StructField("browser",      StringType(),  True),
    StructField("referrer",     StringType(),  True),
    StructField("duration_ms",  IntegerType(), True),
    StructField("region",       StringType(),  True),
    StructField("event_time",   StringType(),  True),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    .option("subscribe", "clickstream_topic")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 20000)
    .load()
)

parsed = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), CLICK_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_topic", F.lit("clickstream_topic"))
)

sys.path.insert(0, os.path.dirname(__file__))
from schema_validator import validate_and_route
from stream_profiler import generate_profile
from snowflake_writer import write_to_snowflake


def process_batch(batch_df, batch_id: int):
    count = batch_df.count()
    if count == 0:
        return

    logger.info(f"Batch {batch_id}: received {count} clickstream records")

    good_df, bad_df = validate_and_route(batch_df, "click", batch_id)
    write_to_snowflake(good_df, "RAW.CLICKSTREAM_RAW")
    generate_profile(batch_df, "clickstream_topic", batch_id)

    logger.info(f"Batch {batch_id} complete")


(
    parsed.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/tmp/streamguard/checkpoints/clickstream")
    .start()
    .awaitTermination()
)
