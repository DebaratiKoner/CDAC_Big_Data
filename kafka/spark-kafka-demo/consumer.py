from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    sum as _sum,
    count,
    to_timestamp
)

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)


# Create Spark session
spark = (
    SparkSession.builder
    .appName("OrdersStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Define JSON schema
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("category", StringType()),
    StructField("city", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", StringType())
])


# Read data continuously from Kafka
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)


# Convert Kafka value from binary to JSON string
orders = (
    raw
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
    .withColumn(
        "event_time",
        to_timestamp("event_time")
    )
)


# One-minute category-wise aggregation
result = (
    orders
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window("event_time", "1 minute"),
        "category"
    )
    .agg(
        _sum("amount").alias("revenue"),
        count("*").alias("orders")
    )
)


# Display continuously on console
query = (
    result
    .writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)


query.awaitTermination()
