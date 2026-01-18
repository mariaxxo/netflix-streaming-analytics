from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
import os
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from datetime import datetime
from pyspark.sql import SparkSession

schema = StructType() \
    .add("user_id", StringType()) \
    .add("movie_title", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("country", StringType()) \
    .add("genre", StringType()) \
    .add("watch_time", IntegerType()) \
    .add("rating", StringType())

connection_str = os.getenv("EVENTHUB_CONN_STR")
eventhub_name = os.getenv("EVENTHUB_NAME")

spark = SparkSession.builder \
    .appName("netflix-streaming-analytics") \
    .getOrCreate()

sc = spark.sparkContext

ehConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
}

raw_stream = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

streaming_df = raw_stream.select(from_json(col("body").cast("string"), schema).alias("payload")) \
    .select("payload.*") \
    .withWatermark("timestamp", "10 minutes")

streaming_df = streaming_df.filter(
    col("timestamp").isNotNull() &
    col("genre").isNotNull() &
    col("movie_title").isNotNull()
)

trending_genres = streaming_df.groupBy(
    window("timestamp", "10 minutes", "5 minutes"),
    "genre"
).count()

checkpoint_loc = "dbfs:/netflix_project/_checkpoints/genres"
output_loc = "dbfs:/netflix_project/gold/trending_genres"

query = trending_genres.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", checkpoint_loc) \
    .start(output_loc)
