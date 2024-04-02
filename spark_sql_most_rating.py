from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

from dataset.utils import find_absolute_path

spark = SparkSession.Builder().appName("Most Rating").getOrCreate()

scheme = StructType([
    StructField("userId", StringType(), True),
    StructField("movieId", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("ts", IntegerType(), True)
])

df = spark.read.option("sep", "\t").schema(
    scheme).csv(find_absolute_path("u.data"))
df.printSchema()

df.groupBy("movieId").count().orderBy(F.desc("count")).show(20)

spark.stop()
