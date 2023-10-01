# Importing system dependencies
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initializing spark session
spark = SparkSession\
    .builder \
    .appName("OnlineAdProject") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Defining inputSchema 
inputSchema = StructType() \
    .add("campaign_id",StringType()) \
    .add("request_id",StringType()) \
    .add("user_id",StringType()) \
    .add("timestamp",StringType()) \
    .add("user_action",StringType()) \
    .add("expenditure",DoubleType()) \
    .add("auction_cpm",DoubleType()) \
    .add("auction_cpc",DoubleType()) \
    .add("auction_cpa",DoubleType()) \
    .add("target_age_range",StringType()) \
    .add("target_location",StringType()) \
    .add("target_gender",StringType()) \
    .add("target_income_bucket",StringType()) \
    .add("campaign_start_time",StringType()) \
    .add("campaign_end_time",StringType())
    

# Reading input stream from Kafka
inputStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","18.211.252.152:9092") \
    .option("startingOffsets","Latest") \
    .option("subscribe","user-feedback") \
    .load()

#Reading json data based on created inputSchema
dataStream = inputStream.select(from_json(col("value").cast("string"),inputSchema).alias("data")).select("data.*")

# Writing feedback data to HDFS as CSV 
FeedbackHDFS = dataStream \
    .writeStream \
    .format('csv') \
    .outputMode("append") \
    .option("truncate","false") \
    .option("path","output") \
    .option("checkpointLocation","checkpoint") \
    .trigger(processingTime="1 minute") \
    .start()

# Waiting for process to complete
FeedbackHDFS.awaitTermination()

