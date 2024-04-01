from pyspark.sql import SparkSession
from dataset.utils import find_absolute_path

# "local[4]" 中的 4 个线程是由 executor 使用的，用于并行执行任务。driver 程序通常只使用一个线程来控制应用程序的流程。
spark = SparkSession.Builder().appName(
    "Use DataFrame to aggregate").master("local[*]").getOrCreate()

fakefriends = spark.read.option("header", "true").option(
    "inferSchema", "true").csv(find_absolute_path("fakefriends-header.csv"))
fakefriends.createOrReplaceTempView("fakefriends")

spark.sql(
    "select age, round(avg(friends),2) avg_fridends from fakefriends group by age order by avg_fridends desc").show(100)

spark.stop()
