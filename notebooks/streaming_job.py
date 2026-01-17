from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder.getOrCreate()

schema = StructType() \
    .add("user_id", StringType()) \
    .add("movie_title", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("country", StringType()) \
    .add("genre", StringType()) \
    .add("watch_time", IntegerType()) \
    .add("rating", IntegerType())

df = spark.readStream.format("eventhubs") \
    .option("eventhubs.connectionString", "<EVENTHUB_CONN_STR>") \
    .load()

json_df = df.selectExpr("cast(body as string) as json")
data_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

trending = data_df.groupBy(
    window("timestamp", "10 minutes"),
    "genre"
).count()

trending.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/genre_trending") \
    .start("/mnt/delta/genre_trending")
