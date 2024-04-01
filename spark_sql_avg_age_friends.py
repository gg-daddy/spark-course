from pyspark.sql import SparkSession
from dataset.utils import find_absolute_path

spark = SparkSession.Builder().appName(
    "Use DataFrame to aggregate").getOrCreate()

fakefriends = spark.read.option("header", "true").option(
    "inferSchema", "true").csv(find_absolute_path("fakefriends-header.csv"))
fakefriends.createOrReplaceTempView("fakefriends")

spark.sql(
    "select age, round(avg(friends),2) avg_fridends from fakefriends group by age order by avg_fridends desc").show(100)

spark.stop()
