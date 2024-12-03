# # # # from pyspark.sql import SparkSession
# # # # from pyspark.sql.functions import *
# # # # from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# # # # # Create a Spark session
# # # # spark = SparkSession.builder \
# # # #     .appName("Real-Time Transaction Monitoring") \
# # # #     .getOrCreate()

# # # # # Define schema for transactions
# # # # schema = StructType([
# # # #     StructField("timestamp", StringType(), True),
# # # #     StructField("product_id", StringType(), True),
# # # #     StructField("quantity", IntegerType(), True),
# # # #     StructField("price", DoubleType(), True)
# # # # ])

# # # # # Define a function to identify anomalies
# # # # def detect_anomalies(quantity, price):
# # # #     if quantity > 60 or price > 750:  # Example anomaly thresholds
# # # #         return "Anomaly"
# # # #     else:
# # # #         return "Normal"

# # # # # Register the UDF
# # # # anomaly_udf = udf(detect_anomalies, StringType())

# # # # # Stream data from the socket
# # # # lines = spark.readStream \
# # # #     .format("socket") \
# # # #     .option("host", "localhost") \
# # # #     .option("port", 9999) \
# # # #     .load()

# # # # # Parse the input data
# # # # transactions = lines.withColumn("value", split(lines["value"], ",")) \
# # # #     .selectExpr(
# # # #         "value[0] as timestamp",
# # # #         "value[1] as product_id",
# # # #         "CAST(value[2] AS INT) as quantity",
# # # #         "CAST(value[3] AS DOUBLE) as price"
# # # #     )

# # # # # Calculate running total sales
# # # # sales_summary = transactions.groupBy("product_id") \
# # # #     .agg(
# # # #         sum(col("price") * col("quantity")).alias("total_sales"),
# # # #         count("*").alias("transaction_count")
# # # #     )

# # # # # Add anomaly detection column
# # # # transactions_with_anomalies = transactions.withColumn("anomaly", anomaly_udf(col("quantity"), col("price")))

# # # # # Write output to console for debugging
# # # # query_sales = sales_summary.writeStream \
# # # #     .outputMode("update") \
# # # #     .format("console") \
# # # #     .start()

# # # # query_anomalies = transactions_with_anomalies.writeStream \
# # # #     .outputMode("append") \
# # # #     .format("console") \
# # # #     .start()

# # # # # Await termination
# # # # query_sales.awaitTermination()
# # # # query_anomalies.awaitTermination()
# # # # ------------------------------------
# # # # Added by Lucky

# # # from pyspark.sql import SparkSession
# # # from pyspark.sql.functions import split, col, window, count, sum as spark_sum, to_timestamp

# # # # Initialize Spark Session
# # # spark = SparkSession.builder.appName("RealTimeTransactionMonitoring").getOrCreate()

# # # # Suppress unnecessary logs
# # # spark.sparkContext.setLogLevel("ERROR")

# # # # Read data from socket
# # # lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# # # # Parse the incoming data
# # # transactions = lines.withColumn("Timestamp", to_timestamp(split(col("value"), ",").getItem(0), "yyyy-MM-dd HH:mm:ss"))  \
# # #                     .withColumn("Product", split(col("value"), ",").getItem(1)) \
# # #                     .withColumn("Quantity", split(col("value"), ",").getItem(2).cast("int")) \
# # #                     .withColumn("Price", split(col("value"), ",").getItem(3).cast("float"))

# # # transactions.printSchema()

# # # # Add a watermark based on the Timestamp column
# # # transactions_with_watermark = transactions.withWatermark("Timestamp", "5 minutes")
# # # #transactions.writeStream.outputMode("append").format("console").option("truncate", False).start()

# # # # Calculate running total sales per product
# # # running_sales = transactions_with_watermark.groupBy("Product").agg({"Quantity": "sum", "Price": "sum"})
# # # running_sales.printSchema()


# # # # Identify anomalies: quantities > threshold or unusual prices
# # # anomalies = transactions_with_watermark.filter((col("Quantity") > 60) | (col("Price") > 700))
# # # anomalies.printSchema()

# # # #running_sales = transactions.groupBy("Product").agg(
# # # #    spark_sum("Quantity").alias("TotalQuantity"), spark_sum("Price").alias("TotalPrice")
# # # #)
# # # # Calculate running total sales per product
# # # # running_sales = transactions.groupBy("Product").agg(
# # # #     spark_sum("Quantity").alias("sum_Quantity"),
# # # #     spark_sum("Price").alias("sum_Price")
# # # # )

# # # # Write results to SQLite database
# # # def write_to_db(df, epoch_id):
# # #     print(f"Writing running_sales to database for epoch {epoch_id}")
# # #     # Define database connection
# # #     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
# # #     table_name = "running_sales"

# # #     # # Write running sales
# # #     try:
# # #         # Show the DataFrame for debugging
# # #         df.show()
# # #         df.write.format("jdbc") \
# # #             .option("url", db_url) \
# # #             .option("dbtable", table_name) \
# # #             .option("driver", "org.sqlite.JDBC") \
# # #             .mode("append") \
# # #             .save()
# # #         print(f"Data written successfully to {table_name}")
# # #     except Exception as e:
# # #         print(f"Error writing running_sales: {e}")





# # # def write_anomalies_to_db(df, epoch_id):
# # #     # Define database connection
# # #     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
# # #     table_name = "anomalies"
# # #     try:
# # #     # Write anomalies
# # #         df.show()
# # #         df.write.format("jdbc") \
# # #             .option("url", db_url) \
# # #             .option("dbtable", table_name) \
# # #             .option("driver", "org.sqlite.JDBC") \
# # #             .mode("append") \
# # #             .save()
# # #         print(f"Data written successfully to {table_name}")
# # #     except Exception as e:
# # #         print(f"Error writing anomalies: {e}")   
        
# # # query1 = running_sales.writeStream \
# # #     .foreachBatch(write_to_db) \
# # #     .outputMode("append") \
# # #     .start() 

# # # query2 = anomalies.writeStream \
# # #     .foreachBatch(write_anomalies_to_db) \
# # #     .outputMode("append") \
# # #     .start() 
    
    
# # # # # Output running sales and anomalies
# # # # query1 = running_sales.writeStream.outputMode("complete").format("console").start()
# # # # query2 = anomalies.writeStream.outputMode("append").format("console").start()

# # # # Await termination for both queries
# # # query1.awaitTermination()
# # # query2.awaitTermination()
# # # ------------------------------

# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import split, col, sum as spark_sum, to_timestamp

# # # Initialize Spark Session
# # spark = SparkSession.builder.appName("RealTimeTransactionMonitoring").getOrCreate()

# # # Suppress unnecessary logs
# # spark.sparkContext.setLogLevel("ERROR")

# # # Read data from socket
# # lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# # # Parse the incoming data
# # transactions = lines.withColumn("Timestamp", to_timestamp(split(col("value"), ",").getItem(0), "yyyy-MM-dd HH:mm:ss")) \
# #                     .withColumn("Product", split(col("value"), ",").getItem(1)) \
# #                     .withColumn("Quantity", split(col("value"), ",").getItem(2).cast("int")) \
# #                     .withColumn("Price", split(col("value"), ",").getItem(3).cast("float"))

# # transactions.printSchema()

# # # Add a watermark based on the Timestamp column
# # transactions_with_watermark = transactions.withWatermark("Timestamp", "1 seconds")

# # # Calculate running total sales per product with watermark
# # # running_sales = transactions_with_watermark.groupBy("Product").agg(
# # #     spark_sum("Quantity").alias("TotalQuantity"),
# # #     spark_sum("Price").alias("TotalSales")
# # # )

# # running_sales = transactions_with_watermark.groupBy("Product").agg({"Quantity": "sum", "Price": "sum"})

# # # Identify anomalies: quantities > threshold or unusual prices
# # anomalies = transactions.filter((col("Quantity") > 60) | (col("Price") > 700))

# # # Function to write running_sales to SQLite
# # def write_running_sales_to_db(df, epoch_id):
# #     print(f"Writing running_sales to database for epoch {epoch_id}")
# #     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
# #     table_name = "running_sales"

# #     try:
# #         # Show the DataFrame for debugging
# #         df.show()
# #         # Write the DataFrame to the SQLite database
# #         df.write.format("jdbc") \
# #             .option("url", db_url) \
# #             .option("dbtable", table_name) \
# #             .option("driver", "org.sqlite.JDBC") \
# #             .mode("append") \
# #             .save()
# #         print(f"Data written successfully to {table_name}")
# #     except Exception as e:
# #         print(f"Error writing running_sales: {e}")

# # # Function to write anomalies to SQLite
# # def write_anomalies_to_db(df, epoch_id):
# #     print(f"Writing anomalies to database for epoch {epoch_id}")
# #     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
# #     table_name = "anomalies"

# #     try:
# #         # Show the DataFrame for debugging
# #         df.show()
# #         # Write the DataFrame to the SQLite database
# #         df.write.format("jdbc") \
# #             .option("url", db_url) \
# #             .option("dbtable", table_name) \
# #             .option("driver", "org.sqlite.JDBC") \
# #             .mode("append") \
# #             .save()
# #         print(f"Data written successfully to {table_name}")
# #     except Exception as e:
# #         print(f"Error writing anomalies: {e}")

# # # Write running_sales to the database
# # query1 = running_sales.writeStream \
# #     .foreachBatch(write_running_sales_to_db) \
# #     .outputMode("complete") \
# #     .start() \
# #     .awaitTermination()
    

# # # Write anomalies to the database
# # query2 = anomalies.writeStream \
# #     .foreachBatch(write_anomalies_to_db) \
# #     .outputMode("append") \
# #     .start()

# # # Await termination for both queries
# # # query1.awaitTermination()
# # query2.awaitTermination()


# # ------------------------------

# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import split, col, sum as spark_sum, to_timestamp

# # # Initialize Spark Session
# # spark = SparkSession.builder.appName("RealTimeTransactionMonitoring").getOrCreate()

# # # Suppress unnecessary logs
# # spark.sparkContext.setLogLevel("ERROR")

# # # Read data from socket
# # lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# # # Parse the incoming data
# # transactions = lines.withColumn("Timestamp", to_timestamp(split(col("value"), ",").getItem(0), "yyyy-MM-dd HH:mm:ss")) \
# #                     .withColumn("Product", split(col("value"), ",").getItem(1)) \
# #                     .withColumn("Quantity", split(col("value"), ",").getItem(2).cast("int")) \
# #                     .withColumn("Price", split(col("value"), ",").getItem(3).cast("float"))

# # # Add a watermark based on the Timestamp column
# # transactions_with_watermark = transactions.withWatermark("Timestamp", "1 minute")

# # # Calculate running total sales per product with watermark
# # running_sales = transactions.groupBy("Product").agg(
# #     spark_sum("Quantity").alias("TotalQuantity"),
# #     spark_sum("Price").alias("TotalSales")
# # )

# # # Identify anomalies: quantities > threshold or unusual prices
# # anomalies = transactions.filter((col("Quantity") > 60) | (col("Price") > 700))

# # # Function to write running_sales to SQLite
# # def write_running_sales_to_db(df, epoch_id):
# #     print(f"Writing running_sales to database for epoch {epoch_id}")
# #     print("Is query1 active?", query1.isActive)
# #     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
# #     table_name = "running_sales"

# #     try:
# #         # Show the DataFrame for debugging
# #         df.show()
# #         # Write the DataFrame to the SQLite database
# #         df.write.format("jdbc") \
# #             .option("url", db_url) \
# #             .option("dbtable", table_name) \
# #             .option("driver", "org.sqlite.JDBC") \
# #             .mode("append") \
# #             .save()
# #         print(f"Data written successfully to {table_name}")
# #     except Exception as e:
# #         print(f"Error writing running_sales: {e}")

# # # Function to write anomalies to SQLite
# # def write_anomalies_to_db(df, epoch_id):
# #     print(f"Writing anomalies to database for epoch {epoch_id}")
# #     print("Is query2 active?", query2.isActive)
# #     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
# #     table_name = "anomalies"

# #     try:
# #         # Show the DataFrame for debugging
# #         df.show()
# #         # Write the DataFrame to the SQLite database
# #         df.write.format("jdbc") \
# #             .option("url", db_url) \
# #             .option("dbtable", table_name) \
# #             .option("driver", "org.sqlite.JDBC") \
# #             .mode("append") \
# #             .save()
# #         print(f"Data written successfully to {table_name}")
# #     except Exception as e:
# #         print(f"Error writing anomalies: {e}")

# # # Write running_sales to the database


# # # Write anomalies to the database
# # query2 = anomalies.writeStream \
# #     .foreachBatch(write_anomalies_to_db) \
# #     .outputMode("append") \
# #     .start()
    
# # query1 = running_sales.writeStream \
# #     .foreachBatch(write_running_sales_to_db) \
# #     .outputMode("update") \
# #     .start()

# # # Monitor both queries
# # query1_status = query1.lastProgress
# # query2_status = query2.lastProgress

# # print("Running Sales Query Status:", query1_status)
# # print("Anomalies Query Status:", query2_status)

# # spark.streams.awaitAnyTermination()

# # # Await termination for both queries
# # query1.awaitTermination()
# # query2.awaitTermination()

# # Wait for any query to terminate
# # --------------------------------


# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import split, col, sum as spark_sum, to_timestamp

# # # Initialize Spark Session
# # spark = SparkSession.builder.appName("RealTimeTransactionMonitoring").getOrCreate()

# # # Suppress unnecessary logs
# # spark.sparkContext.setLogLevel("ERROR")

# # # Read data from socket
# # lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# # # Parse the incoming data
# # transactions = lines.withColumn("Timestamp", to_timestamp(split(col("value"), ",").getItem(0), "yyyy-MM-dd HH:mm:ss")) \
# #                     .withColumn("Product", split(col("value"), ",").getItem(1)) \
# #                     .withColumn("Quantity", split(col("value"), ",").getItem(2).cast("int")) \
# #                     .withColumn("Price", split(col("value"), ",").getItem(3).cast("float"))

# # # Add a watermark based on the Timestamp column
# # transactions_with_watermark = transactions.withWatermark("Timestamp", "1 minute")

# # # Calculate running total sales per product with watermark
# # running_sales = transactions_with_watermark.groupBy("Product").agg(
# #     spark_sum("Quantity").alias("TotalQuantity"),
# #     spark_sum("Price").alias("TotalSales")
# # )

# # # Identify anomalies: quantities > threshold or unusual prices
# # anomalies = transactions.filter((col("Quantity") > 60) | (col("Price") > 700))

# # # Debugging: Print intermediate results of running_sales
# # running_sales.writeStream \
# #     .format("console") \
# #     .outputMode("update") \
# #     .option("truncate", "false") \
# #     .start()

# # # Function to write running_sales to SQLite
# # def write_running_sales_to_db(df, epoch_id):
# #     print(f"Writing running_sales to database for epoch {epoch_id}")
# #     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
# #     table_name = "running_sales"

# #     try:
# #         # Show the DataFrame for debugging
# #         df.show()
# #         # Write the DataFrame to the SQLite database
# #         df.write.format("jdbc") \
# #             .option("url", db_url) \
# #             .option("dbtable", table_name) \
# #             .option("driver", "org.sqlite.JDBC") \
# #             .mode("append") \
# #             .save()
# #         print(f"Data written successfully to {table_name}")
# #     except Exception as e:
# #         print(f"Error writing running_sales: {e}")

# # # Function to write anomalies to SQLite
# # def write_anomalies_to_db(df, epoch_id):
# #     print(f"Writing anomalies to database for epoch {epoch_id}")
# #     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
# #     table_name = "anomalies"

# #     try:
# #         # Show the DataFrame for debugging
# #         df.show()
# #         # Write the DataFrame to the SQLite database
# #         df.write.format("jdbc") \
# #             .option("url", db_url) \
# #             .option("dbtable", table_name) \
# #             .option("driver", "org.sqlite.JDBC") \
# #             .mode("append") \
# #             .save()
# #         print(f"Data written successfully to {table_name}")
# #     except Exception as e:
# #         print(f"Error writing anomalies: {e}")

# # # Write running_sales to the database
# # query1 = running_sales.writeStream \
# #     .foreachBatch(write_running_sales_to_db) \
# #     .outputMode("update") \
# #     .start()

# # # Write anomalies to the database
# # query2 = anomalies.writeStream \
# #     .foreachBatch(write_anomalies_to_db) \
# #     .outputMode("append") \
# #     .start()

# # # Monitor both queries
# # spark.streams.awaitAnyTermination()
# # -------------------------------


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import split, col, sum as spark_sum, to_timestamp
# import time
# import signal
# import sys

# # Initialize Spark Session
# # spark = SparkSession.builder.appName("RealTimeTransactionMonitoring").getOrCreate()

# spark = SparkSession.builder \
#     .appName("RealTimeTransactionMonitoring") \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.executor.cores", "2") \
#     .getOrCreate()


# # Suppress unnecessary logs
# spark.sparkContext.setLogLevel("ERROR")


# # Handle graceful termination
# def signal_handler(sig, frame):
#     print("Termination signal received. Stopping queries...")
#     if query1.isActive:
#         query1.stop()
#     if query2.isActive:
#         query2.stop()
#     print("Queries stopped. Exiting...")
#     sys.exit(0)

# signal.signal(signal.SIGINT, signal_handler)

# # Read data from socket
# lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# # Parse the incoming data
# transactions = lines.withColumn("Timestamp", to_timestamp(split(col("value"), ",").getItem(0), "yyyy-MM-dd HH:mm:ss")) \
#                     .withColumn("Product", split(col("value"), ",").getItem(1)) \
#                     .withColumn("Quantity", split(col("value"), ",").getItem(2).cast("int")) \
#                     .withColumn("Price", split(col("value"), ",").getItem(3).cast("float"))
                        


# # # Cache the source stream to allow multiple independent queries
# # cached_transactions = transactions.cache()

# # Create separate streams for running_sales and anomalies
# transactions_for_running_sales = transactions.withWatermark("Timestamp", "30 seconds")
# transactions_for_anomalies = transactions.withWatermark("Timestamp", "10 seconds")
# # # Add a watermark based on the Timestamp column (set to 10 seconds to match data interval)
# # transactions_with_watermark = cached_transactions.withWatermark("Timestamp", "20 seconds")

# # Calculate running total sales per product with watermark
# running_sales = transactions_for_running_sales.groupBy("Product").agg(
#     spark_sum("Quantity").alias("TotalQuantity"),
#     spark_sum("Price").alias("TotalSales")
# )

# # Identify anomalies: quantities > threshold or unusual prices
# anomalies = transactions_for_anomalies.filter((col("Quantity") > 60) | (col("Price") > 700))

# # Debugging: Print intermediate results of running_sales
# running_sales.writeStream \
#     .format("console") \
#     .outputMode("complete") \
#     .option("truncate", "false") \
#     .start()

# # Function to write running_sales to SQLite
# def write_running_sales_to_db(df, epoch_id):
#     print(f"Writing running_sales to database for epoch {epoch_id}")
#     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
#     table_name = "running_sales"

#     try:
#         # Show the DataFrame for debugging
#         df.show()
#         # Write the DataFrame to the SQLite database
#         df.write.format("jdbc") \
#             .option("url", db_url) \
#             .option("dbtable", table_name) \
#             .option("driver", "org.sqlite.JDBC") \
#             .mode("append") \
#             .save()
#         print(f"Data written successfully to {table_name}")
#     except Exception as e:
#         print(f"Error writing running_sales: {e}")

# # Function to write anomalies to SQLite
# def write_anomalies_to_db(df, epoch_id):
#     print(f"Writing anomalies to database for epoch {epoch_id}")
#     db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
#     table_name = "anomalies"

#     try:
#         # Show the DataFrame for debugging
#         df.show()
#         # Write the DataFrame to the SQLite database
#         df.write.format("jdbc") \
#             .option("url", db_url) \
#             .option("dbtable", table_name) \
#             .option("driver", "org.sqlite.JDBC") \
#             .mode("append") \
#             .save()
#         print(f"Data written successfully to {table_name}")
#     except Exception as e:
#         print(f"Error writing anomalies: {e}")

# # Write running_sales to the database
# query1 = running_sales.writeStream \
#     .foreachBatch(write_running_sales_to_db) \
#     .outputMode("complete") \
#     .trigger(processingTime="15 seconds") \
#     .start()

# # Write anomalies to the database
# query2 = anomalies.writeStream \
#     .foreachBatch(write_anomalies_to_db) \
#     .outputMode("append") \
#     .trigger(processingTime="15 seconds") \
#     .start()

# # Monitor both queries
# while True:
#     query1_status = query1.status
#     query2_status = query2.status
#     print("Query1 Status:", query1_status)
#     print("Query2 Status:", query2_status)

#     if not query1.isActive or not query2.isActive:
#         print("One of the queries has stopped. Terminating...")
#         break

#     # Wait to check statuses periodically
#     import time
#     time.sleep(5)

# spark.streams.awaitAnyTermination()


from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, sum as spark_sum, to_timestamp
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RealTimeTransactionMonitoring") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

# Suppress unnecessary logs
spark.sparkContext.setLogLevel("ERROR")

# Read data from socket
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse the incoming data
transactions = lines.withColumn("Timestamp", to_timestamp(split(col("value"), ",").getItem(0), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("Product", split(col("value"), ",").getItem(1)) \
    .withColumn("Quantity", split(col("value"), ",").getItem(2).cast("int")) \
    .withColumn("Price", split(col("value"), ",").getItem(3).cast("float"))

# Add a watermark based on the Timestamp column
transactions_with_watermark = transactions.withWatermark("Timestamp", "1 minute")

# Calculate running total sales per product and add TotalSales column
running_sales = transactions_with_watermark.groupBy("Product").agg(
    spark_sum("Quantity").alias("TotalQuantity"),
    spark_sum(col("Quantity") * col("Price")).alias("TotalSales")
)

# Identify anomalies: quantities > threshold or unusual prices
anomalies = transactions_with_watermark.filter(
    (col("Quantity") > 60) | (col("Price") > 700)
)

# Function to write running_sales to SQLite
def write_running_sales_to_db(df, epoch_id):
    print(f"Writing running_sales to database for epoch {epoch_id}")
    db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
    table_name = "running_sales"
    try:
        df.show()  # Debugging output
        df.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("driver", "org.sqlite.JDBC") \
            .mode("append") \
            .save()
        print(f"Data written successfully to {table_name}")
    except Exception as e:
        print(f"Error writing running_sales: {e}")

# Function to write anomalies to SQLite
def write_anomalies_to_db(df, epoch_id):
    print(f"Writing anomalies to database for epoch {epoch_id}")
    db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
    table_name = "anomalies"
    try:
        df.show()  # Debugging output
        df.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("driver", "org.sqlite.JDBC") \
            .mode("append") \
            .save()
        print(f"Data written successfully to {table_name}")
    except Exception as e:
        print(f"Error writing anomalies: {e}")

# Write running_sales to the database
query1 = running_sales.writeStream \
    .foreachBatch(write_running_sales_to_db) \
    .outputMode("update") \
    .trigger(processingTime="5 seconds") \
    .start()

# Write anomalies to the database
query2 = anomalies.writeStream \
    .foreachBatch(write_anomalies_to_db) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

# Monitor query progress
try:
    while query1.isActive or query2.isActive:
        print("Query1 Status:", query1.lastProgress)
        print("Query2 Status:", query2.lastProgress)
        time.sleep(5)
except KeyboardInterrupt:
    print("Stopping streaming queries...")
    query1.stop()
    query2.stop()

spark.stop()
