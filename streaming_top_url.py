from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_extract

spark = SparkSession.Builder().appName("StructuredStreaming").getOrCreate()

access_lines = spark.readStream.text("./dataset/logs")

generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'

df_logs = access_lines.select(regexp_extract(
    'value', generalExp, 2).alias('endpoint'),)

df_logs = df_logs.withColumn("event_time", F.current_timestamp())

df_result = (
    df_logs.groupBy(
        F.window(F.col("event_time"), "30 seconds", "10 seconds"),
        F.col("endpoint"))
    .count()
    .alias("count")
    .orderBy(F.col("count").desc())
)

query = (
    df_result.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", "50")
    .queryName("top_url")
    .start()
)

query.awaitTermination()

# Cleanly shut down the session
spark.stop()
