from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("JSONLoader").getOrCreate()

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

#online_retail_df['InvoiceDate'] = pd.to_datetime(online_retail_df['InvoiceDate'], format='%Y-%m-%d %H:%M:%S')

# Display the data
online_retail_df.show()

print(online_retail_df.describe())

spark.stop()
