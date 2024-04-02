from pyspark.sql import SparkSession, functions as F
from dataset.utils import find_absolute_path
import codecs


spark = SparkSession.Builder().appName(
    "Most Rating Using boradcasting to map id to name!").getOrCreate()


def load_move_names():
    '''
    u.item sample data:
    1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
    '''
    movie_names = {}
    with codecs.open(find_absolute_path("u.item"), "r", "ISO-8859-1") as f:
        for line in f:
            fields = line.split("|")
            movie_names[fields[0]] = fields[1]
    return movie_names


movie_names = spark.sparkContext.broadcast(load_move_names())


def get_movie_name(movie_id):
    return movie_names.value.get(movie_id, "Unknown")


to_movie_name = F.udf(get_movie_name)


# 下面这种定义 scheme 的方式也是可以的，相比 StructType 更加简洁。
scheme = "userId STRING, movieId STRING, rating INT, ts INT"
df = spark.read.option("sep", "\t").schema(
    scheme).csv(find_absolute_path("u.data"))

df.printSchema()


df.groupBy("movieId")\
    .count()\
    .orderBy(F.desc("count"))\
    .withColumn("movie_name", to_movie_name(F.col("movieId")))\
    .select("movieId", "movie_name", "count")\
    .show(20, truncate=False)

spark.stop()
