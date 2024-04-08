from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.Builder().appName("StructuredStreaming").getOrCreate()

access_lines = spark.readStream.text("./dataset/logs")

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

df_logs = access_lines.select(
    regexp_extract('value', hostExp, 1).alias('host'),
    regexp_extract('value', timeExp, 1).alias('timestamp'),
    regexp_extract('value', generalExp, 1).alias('method'),
    regexp_extract('value', generalExp, 2).alias('endpoint'),
    regexp_extract('value', generalExp, 3).alias('protocol'),
    regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
    regexp_extract('value', contentSizeExp, 1).cast(
        'integer').alias('content_size')
)

df_status_counts = df_logs.groupBy(df_logs.status).count()
'''
长行分割：在 Python 中，如果一行代码太长，我们可以使用括号将其分割成多行，以提高代码的可读性。
在这个例子中，括号允许我们将 writeStream 链式调用的每个方法写在单独的一行上，而不需要在每一行的末尾添加反斜杠 \。这样做的好处是代码更易读，更易维护。
'''
query = (
    df_status_counts.writeStream
    .outputMode("complete")
    .format("console")
    .queryName("counts")
    .start()
)

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()
