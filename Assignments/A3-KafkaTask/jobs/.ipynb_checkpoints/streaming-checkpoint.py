import kafka
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("average_speed", DoubleType(), True),
        StructField("congestion_level", StringType(), True),
    ]
)
spark = (
    SparkSession.builder.appName("RealtimeTraficAnalysis")
    # .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)  # type: ignore[attr-defined] - Pylance false positive

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:29092")
    .option("subscribe", "trafic_data")
    .option("startingOffsets", "earliest")
    .load()
)
raw_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format(
    "console"
).outputMode("append").start()
# Parse JSON data
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write stream to console for testing
query = parsed_df.writeStream.outputMode("append").format("console").start()

spark.streams.awaitAnyTermination()


# # streaming.py

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     from_json, col, window, expr, avg, sum as _sum,
#     when, lag, desc
# )
# from pyspark.sql.types import *
# from pyspark.sql.window import Window

# # Define schema for JSON data
# schema = StructType([
#     StructField("sensor_id", StringType(), True),
#     StructField("timestamp", StringType(), True),
#     StructField("vehicle_count", IntegerType(), True),
#     StructField("average_speed", DoubleType(), True),
#     StructField("congestion_level", StringType(), True)
# ])

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("RealTimeTrafficAnalysis") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Read from Kafka
# raw_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "traffic_data") \
#     .option("startingOffsets", "latest") \
#     .load()

# # Parse JSON values
# parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*") \
#     .withColumn("event_time", col("timestamp").cast("timestamp"))

# # 🚗 1. Traffic Volume per Sensor (5 min window)
# traffic_volume = parsed_df \
#     .groupBy(
#         window(col("event_time"), "5 minutes"),
#         col("sensor_id")
#     ) \
#     .agg(_sum("vehicle_count").alias("total_count"))

# # 🚦 2. Congestion Hotspots (3 consecutive HIGHs)
# congestion_window = parsed_df \
#     .withWatermark("event_time", "10 minutes") \
#     .groupBy(
#         window(col("event_time"), "5 minutes", "1 minute"),
#         col("sensor_id")
#     ) \
#     .agg(expr("collect_list(congestion_level)").alias("congestion_list")) \
#     .filter(expr("""
#         size(filter(congestion_list, x -> x = 'HIGH')) >= 3
#     """))

# # 🕓 3. Average Speed per Sensor (10-min rolling window)
# avg_speed = parsed_df \
#     .withWatermark("event_time", "15 minutes") \
#     .groupBy(
#         window(col("event_time"), "10 minutes", "1 minute"),
#         col("sensor_id")
#     ) \
#     .agg(avg("average_speed").alias("avg_speed"))

# # ⚠️ 4. Sudden Speed Drop Detection (drop > 50% within 2 mins)
# speed_window = Window.partitionBy("sensor_id").orderBy("event_time")

# speed_diff = parsed_df \
#     .withColumn("prev_speed", lag("average_speed").over(speed_window)) \
#     .withColumn("speed_drop",
#                 when((col("prev_speed").isNotNull()) &
#                      (col("average_speed") < col("prev_speed") * 0.5), True).otherwise(False)) \
#     .filter(col("speed_drop") == True)

# # 📊 5. Top 3 Busiest Sensors (last 30 mins)
# top_sensors = parsed_df \
#     .withWatermark("event_time", "30 minutes") \
#     .groupBy(
#         window(col("event_time"), "30 minutes"),
#         col("sensor_id")
#     ) \
#     .agg(_sum("vehicle_count").alias("total_vehicles")) \
#     .orderBy(desc("total_vehicles")) \
#     .limit(3)

# # Example: write traffic_volume to console for testing
# traffic_volume.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# # You can also write other queries to Kafka or sink separately like:
# # avg_speed.writeStream.format("kafka").option(...)...

# spark.streams.awaitAnyTermination()
