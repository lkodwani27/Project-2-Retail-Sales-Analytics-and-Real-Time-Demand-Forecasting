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
from pyspark.sql.functions import split, col, window, count, sum as spark_sum

# Initialize Spark Session
spark = SparkSession.builder.appName("RealTimeTransactionMonitoring").getOrCreate()

# Suppress unnecessary logs
spark.sparkContext.setLogLevel("ERROR")

# Read data from socket
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse the incoming data
transactions = lines.withColumn("Timestamp", split(col("value"), ",").getItem(0)) \
                    .withColumn("Product", split(col("value"), ",").getItem(1)) \
                    .withColumn("Quantity", split(col("value"), ",").getItem(2).cast("int")) \
                    .withColumn("Price", split(col("value"), ",").getItem(3).cast("float"))

transactions.printSchema()
#transactions.writeStream.outputMode("append").format("console").option("truncate", False).start()

# Calculate running total sales per product
running_sales = transactions.groupBy("Product").agg({"Quantity": "sum", "Price": "sum"})
running_sales.printSchema()
#running_sales = transactions.groupBy("Product").agg(
#    spark_sum("Quantity").alias("TotalQuantity"), spark_sum("Price").alias("TotalPrice")
#)
# Calculate running total sales per product
# running_sales = transactions.groupBy("Product").agg(
#     spark_sum("Quantity").alias("sum_Quantity"),
#     spark_sum("Price").alias("sum_Price")
# )

# Rename columns explicitly to match expected schema
# running_sales = running_sales.select(
#     col("Product"),
#     col("sum(Quantity)").alias("TotalQuantity"),
#     col("sum(Price)").alias("TotalSales")
# )

# Write results to SQLite database
def write_to_db(df, epoch_id):
    print(f"Writing running_sales to database for epoch {epoch_id}")
    # Define database connection
    db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
    table_name = "running_sales"

    # # Write running sales
    # df.write.format("jdbc") \
    #     .option("url", db_url) \
    #     .option("dbtable", table_name) \
    #     .option("driver", "org.sqlite.JDBC") \
    #     .mode("append") \
    #     .save()
    try:
        # Show the DataFrame for debugging
        df.show()
        df.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("driver", "org.sqlite.JDBC") \
            .mode("append") \
            .save()
        print(f"Data written successfully to {table_name}")
    except Exception as e:
        print(f"Error writing running_sales: {e}")



# Identify anomalies: quantities > threshold or unusual prices
anomalies = transactions.filter((col("Quantity") > 60) | (col("Price") > 700))

def write_anomalies_to_db(df, epoch_id):
    # Define database connection
    db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
    table_name = "anomalies"

    # Write anomalies
    df.write.format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table_name) \
        .option("driver", "org.sqlite.JDBC") \
        .mode("append") \
        .save()
        
running_sales.writeStream \
    .foreachBatch(write_to_db) \
    .outputMode("complete") \
    .start() \
    .awaitTermination()        

anomalies.writeStream \
    .foreachBatch(write_anomalies_to_db) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
    
# # Output running sales and anomalies
# query1 = running_sales.writeStream.outputMode("complete").format("console").start()
# query2 = anomalies.writeStream.outputMode("append").format("console").start()

# query1.awaitTermination()
# query2.awaitTermination()
