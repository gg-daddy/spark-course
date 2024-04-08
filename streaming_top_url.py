from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_extract, to_timestamp

spark = SparkSession.Builder().appName("StructuredStreaming").getOrCreate()

access_lines = spark.readStream.text("./dataset/logs")

generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\]'

df_logs = access_lines.select(
    to_timestamp(
        regexp_extract('value', timeExp, 1),
        "dd/MMM/yyyy:HH:mm:ss Z"
    ).alias('event_time'),
    regexp_extract('value', generalExp, 2).alias('endpoint'),
)

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
