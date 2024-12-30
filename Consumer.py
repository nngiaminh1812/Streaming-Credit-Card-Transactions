from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, lit, regexp_replace, lpad
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from rate_VND import exchange_VND
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName('Consumer') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.0") \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .getOrCreate()

streamed_source_kafka = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', '127.0.0.1:9092') \
    .option('subscribe', 'stream_credits') \
    .option('startingOffsets', 'latest') \
    .load()

checkpointDir = '/run/media/nngiaminh1812/Data/ODAP/Final/checkpoint/'

rate = exchange_VND()
schema = StructType([
    StructField("User", StringType(), True),
    StructField("Card", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Day", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("Use Chip", StringType(), True),
    StructField("Merchant Name", StringType(), True),
    StructField("Merchant City", StringType(), True),
    StructField("Merchant State", StringType(), True),
    StructField("Zip", StringType(), True),
    StructField("MCC", StringType(), True),
    StructField("Errors?", StringType(), True),
    StructField("Is Fraud?", StringType(), True)
])

# offset = streamed_source_kafka.selectExpr("CAST(key AS STRING)")
# print(f"Current offset :{offset}")

df = streamed_source_kafka.selectExpr("CAST(value AS STRING)")
# df.printSchema()
# df.writeStream.format("console").start()

parsed_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed_df.printSchema()
parsed_df.writeStream.format("console").start()

# Add logging to check the contents of parsed_df
parsed_df.writeStream \
    .outputMode('append') \
    .format('console') \
    .option('truncate', False) \
    .start()

# UDF to convert string to integer, returns None if conversion fails
def safe_cast_int(value):
    try:
        return int(value)
    except ValueError:
        return None

safe_cast_int_udf = udf(safe_cast_int, IntegerType())

preprocessed = parsed_df \
    .filter(col("Is Fraud?") != 'Yes') \
    .withColumn("Date", concat_ws('/', 
                                  lpad(col("Day"), 2, '0'), 
                                  lpad(col("Month"), 2, '0'), 
                                  col("Year"))) \
    .withColumn("Time", concat_ws(":", col("Time"), lit("00"))) \
    .withColumn("Amount", regexp_replace(col("Amount"), "\\$", "").cast("double")) \
    .withColumn("Amount", (rate * col("Amount")).cast("double")) \
    .withColumn("User", safe_cast_int_udf(col("User"))) \
    .withColumn("Card", safe_cast_int_udf(col("Card"))) \
    .withColumn("Zip", col("Zip").cast("double")) \
    .withColumn("MCC", safe_cast_int_udf(col("MCC"))) \
    .drop("Year", "Month", "Day") \
    .dropna()

preprocessed.printSchema()

# Add logging to check the contents of preprocessed
preprocessed.writeStream \
    .outputMode('append') \
    .format('console') \
    .option('truncate', False) \
    .start()

query = preprocessed.writeStream \
    .outputMode('append') \
    .format('csv') \
    .option('path', 'hdfs://localhost:9000/user/nngiaminh1812/hive/warehouse/uses_of_credits.db/credit_card_transactions') \
    .option('checkpointLocation', checkpointDir) \
    .option('header', 'true') \
    .start()

query.awaitTermination()