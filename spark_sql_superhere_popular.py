from pyspark.sql import SparkSession, functions as F
from dataset.utils import find_absolute_path


spark = SparkSession.Builder().appName("Superhero - Most").getOrCreate()

df_marvel_names = spark.read.schema("id INT, name STRING").option("sep", " ").csv(
    find_absolute_path("Marvel_Names.csv"))

df_connection = spark.read.text(find_absolute_path("Marvel_Graph.txt"))
most_connection_hero_id = df_connection.withColumn("id", F.split(df_connection.value, " ")[0].cast("int"))\
    .withColumn("connections", F.size(F.split(df_connection.value, " ")) - 1)\
    .groupBy("id")\
    .agg(F.sum("connections").alias("connections"))\
    .orderBy(F.desc("connections"))\
    .first()

most_here_name = df_marvel_names.filter(
    df_marvel_names.id == most_connection_hero_id.id).first()
print(f"{most_here_name.name} has the most connections with {most_connection_hero_id.connections}")

spark.stop()
