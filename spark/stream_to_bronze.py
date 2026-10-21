"""Spark Structured Streaming: Kafka topic -> Parquet files (bronze layer)."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = os.getenv("TOPIC", "iot.readings")
OUT_PATH = os.getenv("BRONZE_OUT", "/data/bronze/iot_readings")
CHECKPOINT = os.getenv("CHECKPOINT", "/data/checkpoints/iot_readings")

schema = T.StructType(
    [
        T.StructField("sensor_id", T.StringType()),
        T.StructField("location", T.StringType()),
        T.StructField("event_ts", T.StringType()),
        T.StructField("temperature_c", T.DoubleType()),
        T.StructField("humidity_pct", T.DoubleType()),
        T.StructField("battery_pct", T.DoubleType()),
    ]
)

spark = (
    SparkSession.builder.appName("iot-kafka-to-bronze")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

bronze = (
    raw.select(
        F.col("timestamp").alias("kafka_ts"),
        F.from_json(F.col("value").cast("string"), schema).alias("j"),
    )
    .select("kafka_ts", "j.*")
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("event_date", F.to_date("event_ts"))
    .where(F.col("event_ts").isNotNull())  # drop unparseable messages
)

query = (
    bronze.writeStream.format("parquet")
    .option("path", OUT_PATH)
    .option("checkpointLocation", CHECKPOINT)
    .partitionBy("event_date")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)
query.awaitTermination()
