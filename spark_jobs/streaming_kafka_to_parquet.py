from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, round
from pyspark.sql.functions import from_unixtime, to_date
import os

# Initialize SparkSession
spark = SparkSession.builder.appName('Kafka2Parquet').getOrCreate()

# Environment variables for Kafka and paths
kafka_bootstrap = os.getenv("KAFKA_BROKER", "kafka:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "weather-topic")
checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/data/chkpt/")
parquet_output_dir = os.getenv("PARQUET_OUTPUT_DIR", "/data/parquet/")

# Corrected JSON schema without trailing commas
json_schema = """
name STRING,
main STRUCT<temp:DOUBLE>,
dt BIGINT,
weather ARRAY<STRUCT<
    main: STRING,
    description: STRING
>>
"""

# Read the Kafka stream
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Cast value to string and parse JSON
df2 = df.selectExpr("CAST(value AS STRING) as json_str")
df_json = df2.select(from_json(col("json_str"), json_schema).alias("data")).select("data.*")

df_flat = df_json.select(
    col("name").alias("city"),
    col("main.temp").alias("temp_kelvin"),
    col("dt"),  # keep for transformation only
    expr("weather[0].description").alias("weather_description")
)

# Add the date_time column derived from dt
#df_flat = df_flat.withColumn("date_time", from_unixtime(col("dt")).cast("timestamp"))
df_flat = df_flat.withColumn("date_time", 
    from_unixtime(col("dt")).cast("timestamp") + expr("INTERVAL 5 HOURS 30 MINUTES")
)

# Compute temp_celsius
df_with_celsius = df_flat.withColumn(
    "temp_celsius",
    round(col("temp_kelvin") - 273.15, 2)
)

# Drop the dt column so it's not in final output
df_final = df_with_celsius.drop("dt")

# Write the final DataFrame without dt
query = df_final.writeStream \
    .format("parquet") \
    .option("checkpointLocation", checkpoint_dir) \
    .option("path", parquet_output_dir) \
    .outputMode("append") \
    .trigger(processingTime="2 minutes") \
    .start()

query.awaitTermination()
