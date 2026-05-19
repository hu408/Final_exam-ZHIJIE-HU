from pyspark.sql import SparkSession
import time
import os

BASE_PATH = "/tmp/datalake"
OUTPUT_PATH = "outputs/analytics"
os.makedirs(OUTPUT_PATH, exist_ok=True)

def create_spark():
    return SparkSession.builder \
        .appName("Analytics") \
        .master("local[*]") \
        .getOrCreate()

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    curated = spark.read.parquet(f"{BASE_PATH}/curated/domain=iot")
    curated.createOrReplaceTempView("sensor_data")

    # Q1: Top5 hours with most anomalies
    q1 = spark.sql("""
        SELECT date_trunc('hour', event_time) as hour, COUNT(*) as anomaly_count
        FROM sensor_data
        WHERE is_anomaly = true
        GROUP BY hour
        ORDER BY anomaly_count DESC
        LIMIT 5
    """)
    q1.show()
    q1.write.csv(f"{OUTPUT_PATH}/q1.csv", header=True, mode="overwrite")

    # Q2: per sensor stats
    q2 = spark.sql("""
        SELECT sensor,
               AVG(value) as avg_val,
               MIN(value) as min_val,
               MAX(value) as max_val,
               STDDEV(value) as std_val,
               SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)/COUNT(*) * 100 as anomaly_rate
        FROM sensor_data
        GROUP BY sensor
    """)
    q2.show()
    q2.write.csv(f"{OUTPUT_PATH}/q2.csv", header=True, mode="overwrite")

    # Q3: daily temp mean & anomaly count
    q3 = spark.sql("""
        SELECT date(event_time) as day,
               AVG(value) as avg_temp,
               SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count
        FROM sensor_data
        WHERE sensor = 'temperature'
        GROUP BY day
        ORDER BY day
    """)
    q3.show()
    q3.write.csv(f"{OUTPUT_PATH}/q3.csv", header=True, mode="overwrite")

    # Q4: partition pruning demo
    start = time.time()
    full = spark.sql("SELECT COUNT(*) FROM sensor_data").collect()
    t1 = time.time() - start

    start = time.time()
    filtered = spark.sql("SELECT COUNT(*) FROM sensor_data WHERE sensor='temperature' AND year=2025").collect()
    t2 = time.time() - start

    print(f"Full scan: {t1:.2f}s, Filtered: {t2:.2f}s, Speedup: {t1/t2:.2f}x")

if __name__ == "__main__":
    main()