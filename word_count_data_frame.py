from pyspark.sql import SparkSession, functions as F
from dataset.utils import find_absolute_path
from pyspark.sql import SparkSession, functions as F
from dataset.utils import find_absolute_path

spark = SparkSession.Builder().appName("WordCountByDataFrame").getOrCreate()
df = spark.read.text(find_absolute_path("book.txt"))

'''
使用 DataFrame ，尤其是其中的 functions 来完成 WordCount 任务。 
'''
words = df.select(F.explode(F.split(df.value, r'\W+')).alias("word")) \
    .filter(F.col("word") != "") \
    .select(F.lower(F.col("word")).alias("word")) \
    .groupBy("word").count().sort("count")

words.show(words.count())

# 上面使用 DateFrame 的代码，可以等价地使用 SQL 来完成：
df.createOrReplaceTempView("lines")
sql = """
SELECT LOWER(word) AS word, COUNT(1) AS count
FROM (
    SELECT explode(split(value, r'\W+')) AS word FROM lines
)
WHERE word != ''
GROUP BY LOWER(word)
ORDER BY count ASC
"""
words = spark.sql(sql)
words.show(words.count())


spark.stop()
