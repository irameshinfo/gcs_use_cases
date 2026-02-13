from pyspark.sql import SparkSession

# Create Spark session with BigQuery connector
spark = SparkSession.builder \
    .appName("GCS to BigQuery") \
    .getOrCreate()

# ---------------------------
# 1. Read file from GCS
# ---------------------------

input_path = "gs://rameshsamplebucket/emp.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

df.show()

# ---------------------------
# 2. Write to BigQuery
# ---------------------------

df.write \
  .format("bigquery") \
  .option("table", "ranjanrishi-project.testdataset.test") \
  .option("temporaryGcsBucket", "ramtempbucket7") \
  .mode("append") \
  .save()

print("Data successfully loaded to BigQuery")