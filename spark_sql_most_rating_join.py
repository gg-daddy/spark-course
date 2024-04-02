from pyspark.sql import SparkSession, functions as F
from dataset.utils import find_absolute_path
from pyspark.sql.types import StringType, StructField, StructType


spark = SparkSession.Builder().appName(
    "Most Rating Using Join!").getOrCreate()

scheme = "userId STRING, movieId STRING, rating INT, ts INT"
df_movie_rating_count = spark.read.option("sep", "\t").schema(
    scheme).csv(find_absolute_path("u.data")).groupBy("movieId").count()

df_movie_name = spark.read.csv(find_absolute_path("u.item"),
                               inferSchema=True, header=False, sep="|")
# 只选择前两列
df_movie_name = df_movie_name.select(df_movie_name.columns[:2])\
    .withColumn("movieId", F.col("_c0").cast("String"))\
    .withColumn("movie_name", F.col("_c1").cast("String"))\
    .select("movieId", "movie_name")

df_movie_rating_count.join(df_movie_name, "movieId", how="left").orderBy(
    F.desc("count")).show(50, truncate=False)

spark.stop()
