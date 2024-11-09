from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("Project2").getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

output_dir = "output/"
task2_output = output_dir + "task2.csv"
task3_output = output_dir + "task3.csv"
task4_output = output_dir + "task4.csv"

checkpoint_dir = "checkpoint/task5/"
task5_output = output_dir + "task5.csv"

# Read the JSON file as a DataFrame
train_df = spark.read.json("train.json")
reviwes_df = spark.read.json("test.json")

train_df.show()
reviwes_df.show()

reviwes_df = reviwes_df.withColumn("helpful", F.col("helpful").cast("double"))
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
#standardize the consistent description
online_retail_df = online_retail_df.withColumn("Description", F.upper(F.col("Description")))

#handle special cases in case for better performance for MLlib
#online_retail_df = online_retail_df.filter(~F.col("InvoiceNo").startswith("C"))

online_retail_df.show()

date_df = online_retail_df.withColumn("Month", F.month("InvoiceDate")) \
                                   .withColumn("Year", F.year("InvoiceDate")).withColumn("Day", F.dayofmonth("InvoiceDate"))
                                   
date_df.show()

#online_retail_df.select(F.min("InvoiceDate").alias("EarliestDate"), F.max("InvoiceDate").alias("LatestDate")).show()
#online_retail_df.groupBy(F.to_date("InvoiceDate").alias("Date")).count().orderBy("Date").show()

#-----------------------------------
#Task 2: Sales Data Aggregation and Feature Engineering
#-----------------------------------
def task2(df):
    
    pass



#-----------------------------------
#Task 3: Demand Forecasting Model
#-----------------------------------

def task3(df):
    
    pass


#-----------------------------------
#Task 4: Sentiment Analysis on Customer Reviews
#-----------------------------------
def task4(df):
    
    pass


#-----------------------------------
#Task 5: Real-Time Transaction Monitoring using Spark Streaming
#-----------------------------------
def task5(socket_stream):
    
    #filtered_stream.writeStream.format("csv")\
    #     .option("path", task5_output)\
    #     .option("header", True)\
    #     .option("checkpointLocation", checkpoint_dir)\
    #     .outputMode("append").start()\
    #     .awaitTermination()
    
    pass

task2(online_retail_df)
task3(online_retail_df)
task4(reviwes_df)
#task5()

spark.stop()
