from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("JSONLoader").getOrCreate()

# Read the JSON file as a DataFrame
df = spark.read.json("test.json")

# Display the data
df.show()

spark.stop()
