from pyspark.sql.functions import col, to_date
from pyspark.sql import SparkSession

def create_spark_session():
    """Create Spark session with fallback options for Hive support"""
    try:
        # First try with Hive support
        return SparkSession.builder \
            .appName('BatchETL') \
            .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
    except Exception as e:
        print(f"Warning: Could not initialize Hive support: {str(e)}")
        print("Falling back to Spark without Hive integration")
        return SparkSession.builder \
            .appName('BatchETL') \
            .getOrCreate()

spark = create_spark_session()

# Data sources configuration
faker_csv = '/data/fake_weather_logs.csv'
mysql_url = "jdbc:mysql://mysql:3306/sensor_data"
mysql_props = {
    "user": "root",
    "password": "rootpassword",
    "driver": "com.mysql.cj.jdbc.Driver"
}

def read_and_process_data():
    """Read and process data from both sources"""
    # Read CSV data
    df_csv = spark.read.csv(faker_csv, header=True, inferSchema=True)
    
    # Read MySQL table
    df_mysql = spark.read.jdbc(mysql_url, "sensor_data_table", properties=mysql_props)
    
    # Filter temperature > 20
    df_csv_filtered = df_csv.filter(col("temperature") > 20)
    df_mysql_filtered = df_mysql.filter(col("temperature") > 20)
    
    # Select and unify schema
    return df_csv_filtered.select(
        col("city").alias("location"),
        col("temperature"),
        to_date(col("timestamp")).alias("date")
    ).unionByName(
        df_mysql_filtered.selectExpr(
            "device_id as location",
            "temperature",
            "cast(timestamp as date) as date"
        )
    )

df_combined = read_and_process_data()

# Show count and sample data for verification
print(f"Row count before writing: {df_combined.count()}")
df_combined.show(5)

# Always write to MySQL (primary storage)
df_combined.write.jdbc(
    url=mysql_url,
    table='final_table',
    mode='overwrite',
    properties=mysql_props
)

# Conditionally write to Hive if supported
try:
    df_combined.write.mode("overwrite").saveAsTable("default.final_table")
    print("Hive table created successfully")
    spark.sql("SELECT * FROM default.final_table LIMIT 5").show()
except Exception as e:
    print(f"Warning: Could not write to Hive: {str(e)}")
    print("Data was only written to MySQL")

# Write to Parquet as fallback storage
df_combined.write.mode("overwrite").parquet("/data/output/final_table.parquet")
print("Data written to Parquet as backup")

spark.stop()