from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("Project2").getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Read the JSON file as a DataFrame
reviwes_df = spark.read.json("test.json")

reviwes_df = reviwes_df.withColumn("helpful", F.col("helpful").cast("double"))
#reviwes_df.printSchema()
#print(reviwes_df.describe())

online_retail_df = spark.read.csv("Online-Retail.csv", inferSchema=True, header=True)

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

online_retail_df.printSchema()

#standardize the consistent description
online_retail_df = online_retail_df.withColumn("Description", F.upper(F.col("Description")))

#handle special cases
#online_retail_df = online_retail_df.filter(~F.col("InvoiceNo").startswith("C"))

online_retail_df.show()

#online_retail_df.select(F.min("InvoiceDate").alias("EarliestDate"), F.max("InvoiceDate").alias("LatestDate")).show()

spark.stop()
