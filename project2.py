from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("CSVloader").getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


# Read the JSON file as a DataFrame
df = spark.read.json("test.json")
"""
online_schema = StructType([StructField("InvoiceNo", IntegerType(), False),
                            StructField("StockCode", StringType(), False),
                            StructField("Description", StringType(), False),
                            StructField("Quantity", IntegerType(), False),
                            StructField("InvoiceDate", DateType(), False),
                            StructField("UnitPrice", DoubleType(), False),
                            StructField("CustomerID", IntegerType(), False),
                            StructField("Country", StringType(), False)])
"""

online_retail_df = spark.read.csv("Online-Retail.csv", inferSchema=True, header=True)

online_retail_df = online_retail_df.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
online_retail_df = online_retail_df.withColumn("InvoiceDate", F.col("InvoiceDate").cast(TimestampType()))
online_retail_df.printSchema()

online_retail_df.select(F.min("InvoiceDate").alias("EarliestDate"), F.max("InvoiceDate").alias("LatestDate")).show()

spark.stop()
