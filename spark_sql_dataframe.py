from pyspark.sql import SparkSession
from dataset.utils import find_absolute_path
from pyspark.sql import functions as F

spark = SparkSession.Builder().appName("Woo").getOrCreate()

pepple = spark.read.option("header", "true").option(
    "inferSchema", "true").csv(find_absolute_path("fakefriends-header.csv"))

print("Print Schema:" + "->" * 20)
pepple.printSchema()

print("Select Columns:" + "->" * 20)
pepple.select("name").show()
pepple.select(["name", "age"]).show()
pepple.select("name", "friends").show()
pepple.select(pepple.name, pepple.age + 10).show()

print("Filter :" + "->" * 20)
pepple.filter(pepple["age"] > 21).show()
pepple.filter(pepple.age > 21).show()

pepple.groupBy("age")\
    .agg(F.sum("friends").alias("friends_sum"))\
    .orderBy("friends_sum", ascending=False)\
    .show()

spark.stop()
