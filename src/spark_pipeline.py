from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, min, max, count, sum, to_timestamp, year, month, day, hour
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, BooleanType

# 👉 完全匹配你的 Kafka
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC = "sensor-events"
BASE_PATH = "C:/tmp/datalake"

schema = StructType() \
    .add("sensor", StringType()) \
    .add("value", DoubleType()) \
    .add("unit", StringType()) \
    .add("timestamp", LongType()) \
    .add("source", StringType()) \
    .add("anomaly", BooleanType())

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SensorPipeline") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp") / 1000)) \
        .withColumn("year", year("event_time")) \
        .withColumn("month", month("event_time")) \
        .withColumn("day", day("event_time")) \
        .withColumn("hour", hour("event_time"))

    valid = parsed.filter(
        (col("sensor") == "temperature") & (col("value") >= 15) & (col("value") <= 45) |
        (col("sensor") == "humidity") & (col("value") >= 30) & (col("value") <= 95) |
        (col("sensor") == "pressure") & (col("value") >= 980) & (col("value") <= 1040)
    )

    valid = valid.withColumn("is_anomaly",
        (col("sensor") == "temperature") & (col("value") > 35) |
        (col("sensor") == "humidity") & (col("value") > 90) |
        (col("sensor") == "pressure") & (col("value") < 990) | (col("value") > 1030)
    ).withColumnRenamed("sensor", "sensor_type")

    # 控制台输出（最稳定）
    query = valid.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="2 seconds") \
        .start()

    print("✅ Spark Streaming is running...")
    query.awaitTermination()