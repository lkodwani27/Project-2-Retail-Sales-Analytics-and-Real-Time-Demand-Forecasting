from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Initialize Spark session
spark = SparkSession.builder.appName("Project2").getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

output_dir = "output/"
task2_output_total = output_dir + "task2/task2_total.csv"
task2_output_avgsales = output_dir + "task2/task2_avgsales.csv"
task2_output_season = output_dir + "task2/task2_season.csv"
task3_output = output_dir + "task3/task3.csv"
task4_output = output_dir + "task4/task4.csv"

checkpoint_dir = "checkpoint/task5/"
task5_output = output_dir + "task5.csv"

# Read the JSON file as a DataFrame
train_df = spark.read.json("train.json")
reviwes_df = spark.read.json("test.json")

train_df.show()
reviwes_df.show()

#reviwes_df = reviwes_df.withColumn("helpful", F.col("helpful").cast("double"))
#reviwes_df.printSchema()
#print(reviwes_df.describe())
# Check if any rows have nulls in the 'helpful' column after casting
#invalid_rows = reviwes_df.filter(F.col("sentence").isNull())
# Show rows that could not be cast to double
#invalid_rows.show()

online_retail_df = spark.read.csv("Online-Retail.csv", inferSchema=True, header=True)

#-----------------------------------
#Task 1: Cleaning the Dataset
#-----------------------------------
# Count nulls in each column
online_retail_df.select([F.count(F.when(col(c).isNull(), c)).alias(c) for c in online_retail_df.columns]).show()

# Drop rows with nulls in important columns like CustomerID, Description, etc.
online_retail_df = online_retail_df.dropna(subset=["CustomerID", "Description", "InvoiceDate", "Quantity", "UnitPrice"])
#online_retail_df.select([F.count(F.when(col(c).isNull(), c)).alias(c) for c in online_retail_df.columns]).show()

#drop duplicates
online_retail_df = online_retail_df.dropDuplicates()

#cast the invoice date to timestamp
online_retail_df = online_retail_df.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
online_retail_df = online_retail_df.withColumn("InvoiceDate", F.col("InvoiceDate").cast(TimestampType()))

#check for outliers
online_retail_df.filter((col("Quantity") < 0) | (col("UnitPrice") < 0)).show()

# Drop rows where Quantity or UnitPrice are negative (common outlier check)
online_retail_df = online_retail_df.filter((F.col("Quantity") >= 0) & (F.col("UnitPrice") >= 0))
#online_retail_df.filter((col("Quantity") < 0) | (col("UnitPrice") < 0)).show()
#online_retail_df.printSchema()
#standardize for consistent description characters
online_retail_df = online_retail_df.withColumn("Description", F.upper(F.col("Description")))

#handle special cases in case for better performance for MLlib
#online_retail_df = online_retail_df.filter(~F.col("InvoiceNo").startswith("C"))
online_retail_df.show()

#-----------------------------------
#Task 2: Sales Data Aggregation and Feature Engineering
#-----------------------------------
#total sales per product per month
#get the month and year from invoicedate
online_retail_df = online_retail_df.withColumn("Month", F.month("InvoiceDate")).withColumn("Year", F.year("InvoiceDate"))
    
#make a revenue column, total quantity * price
online_retail_df = online_retail_df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
    
#total sales per product and month. calculated by summing total quantity * price
total_sales_df = online_retail_df.groupBy("StockCode", "Description", "Month", "Year").agg(F.sum("Revenue").alias("TotalSales"))
total_sales_df.show()
    
#average revenue per customer
#get average revnue for each customer id
avg_revnue_df = online_retail_df.groupBy("CustomerID").agg(F.avg("Revenue").alias("AverageRevenue")).orderBy(F.desc("AverageRevenue"))
avg_revnue_df.show()
    
#seasonal patterns for top selling products
#get products by stock code and their highest total revnue as sum
top_products_df = online_retail_df.groupBy("StockCode").agg(F.sum("Revenue").alias("TotalRevenue")).orderBy(F.desc("TotalRevenue"))
    
#join with df to get montly data, group by product and month for each of their total revenue
seasonal_pattern = online_retail_df.join(top_products_df.limit(10), "StockCode").groupBy("StockCode", "Description", "Month").agg(F.sum("Revenue").alias("MonthlySales"))
seasonal_pattern.show()

#feature engineering
#customer liftime value: total revenue per customer
clv_df = online_retail_df.groupBy("CustomerID", "StockCode").agg(F.sum("Revenue").alias("CustomerLifetimeValue"))
clv_df.show()
    
#product popularity: counted by unique transaction made under each stock code
product_popularity = online_retail_df.groupBy("StockCode").agg(F.countDistinct("InvoiceNo").alias("PopularityScore"))
product_popularity.show()
    
#seasonal trends:
#make season column based on month
online_retail_df = online_retail_df.withColumn("Season",
                    F.when(col("Month").isin(12, 1, 2), "Winter") #if month is in one of these numbers
                    .when(col("Month").isin(3, 4, 5), "Spring")
                    .when(col("Month").isin(6, 7, 8), "Summer")
                    .when(col("Month").isin(9, 10, 11), "Fall")
                    )
    
#total revenue of each product and season
seasonal_trends = online_retail_df.groupBy("StockCode", "Season").agg(F.sum("Revenue").alias("SeasonalSales"))
seasonal_trends.show()

#write the aggregated data to the directory as csv files
#try:
#    total_sales_df.write.csv(task2_output_total, header=True)
#    avg_revnue_df.write.csv(task2_output_avgsales, header=True)
#    seasonal_pattern.write.csv(task2_output_season, header=True)
#except ValueError as e:
#    print(f"error {e}")

#-----------------------------------
#Task 3: Demand Forecasting Model
#-----------------------------------
# Join features into a consolidated DataFrame
forecasting_df = total_sales_df.join(clv_df, "StockCode", "left") \
                               .join(product_popularity, "StockCode", "left") \
                               .join(seasonal_trends, "StockCode", "left")

# Fill nulls with 0 or appropriate values for ML training
forecasting_df = forecasting_df.fillna(0)

# Display the consolidated DataFrame structure
forecasting_df.show()


#-----------------------------------
#Task 4: Sentiment Analysis on Customer Reviews
#-----------------------------------


#-----------------------------------
#Task 5: Real-Time Transaction Monitoring using Spark Streaming
#-----------------------------------
    
#filtered_stream.writeStream.format("csv")\
#     .option("path", task5_output)\
#     .option("header", True)\
#     .option("checkpointLocation", checkpoint_dir)\
#     .outputMode("append").start()\
#     .awaitTermination()

spark.stop()
