#--------------------------------------------------------------
#Task 5: Real-Time Transaction Monitoring using Spark Streaming
#--------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Real-Time Transaction Monitoring") \
    .getOrCreate()

# Define schema for transactions
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

# Define a function to identify anomalies
def detect_anomalies(quantity, price):
    if quantity > 40 or price > 600:  # Example anomaly thresholds
        return "Anomaly"
    else:
        return "Normal"

# Register the UDF (user-defined function)
anomaly_udf = udf(detect_anomalies, StringType())

# Stream data from the socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
    
# Parse the input data
transactions = lines.withColumn("value", split(lines["value"], ",")) \
    .selectExpr(
        "value[0] as timestamp",
        "value[1] as product_id",
        "CAST(value[2] AS INT) as quantity",
        "CAST(value[3] AS DOUBLE) as price"
    )
    
# Calculate running total sales
sales_summary = transactions.groupBy("product_id") \
    .agg(
        sum(col("price") * col("quantity")).alias("total_sales"),
        count("*").alias("transaction_count")
    )
