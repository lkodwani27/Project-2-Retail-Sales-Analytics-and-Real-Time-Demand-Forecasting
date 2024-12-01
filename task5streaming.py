# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# # Create a Spark session
# spark = SparkSession.builder \
#     .appName("Real-Time Transaction Monitoring") \
#     .getOrCreate()

# # Define schema for transactions
# schema = StructType([
#     StructField("timestamp", StringType(), True),
#     StructField("product_id", StringType(), True),
#     StructField("quantity", IntegerType(), True),
#     StructField("price", DoubleType(), True)
# ])

# # Define a function to identify anomalies
# def detect_anomalies(quantity, price):
#     if quantity > 60 or price > 750:  # Example anomaly thresholds
#         return "Anomaly"
#     else:
#         return "Normal"

# # Register the UDF
# anomaly_udf = udf(detect_anomalies, StringType())

# # Stream data from the socket
# lines = spark.readStream \
#     .format("socket") \
#     .option("host", "localhost") \
#     .option("port", 9999) \
#     .load()

# # Parse the input data
# transactions = lines.withColumn("value", split(lines["value"], ",")) \
#     .selectExpr(
#         "value[0] as timestamp",
#         "value[1] as product_id",
#         "CAST(value[2] AS INT) as quantity",
#         "CAST(value[3] AS DOUBLE) as price"
#     )

# # Calculate running total sales
# sales_summary = transactions.groupBy("product_id") \
#     .agg(
#         sum(col("price") * col("quantity")).alias("total_sales"),
#         count("*").alias("transaction_count")
#     )

# # Add anomaly detection column
# transactions_with_anomalies = transactions.withColumn("anomaly", anomaly_udf(col("quantity"), col("price")))

# # Write output to console for debugging
# query_sales = sales_summary.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .start()

# query_anomalies = transactions_with_anomalies.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# # Await termination
# query_sales.awaitTermination()
# query_anomalies.awaitTermination()
# ------------------------------------
# Added by Lucky

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, window

# Initialize Spark Session
spark = SparkSession.builder.appName("RealTimeTransactionMonitoring").getOrCreate()

# Read data from socket
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse the incoming data
transactions = lines.withColumn("Timestamp", split(col("value"), ",").getItem(0)) \
                    .withColumn("Product", split(col("value"), ",").getItem(1)) \
                    .withColumn("Quantity", split(col("value"), ",").getItem(2).cast("int")) \
                    .withColumn("Price", split(col("value"), ",").getItem(3).cast("float"))

# Calculate running total sales per product
running_sales = transactions.groupBy("Product").agg({"Quantity": "sum", "Price": "sum"})

# Identify anomalies: quantities > threshold or unusual prices
anomalies = transactions.filter((col("Quantity") > 3) | (col("Price") > 400))

# Output running sales and anomalies
query1 = running_sales.writeStream.outputMode("complete").format("console").start()
query2 = anomalies.writeStream.outputMode("append").format("console").start()

query1.awaitTermination()
query2.awaitTermination()
