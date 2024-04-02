from pyspark.sql import SparkSession, functions as F
from dataset.utils import find_absolute_path


spark = SparkSession.Builder().appName("Superhero - Obscure").getOrCreate()

df_marvel_names = spark.read.schema("id INT, name STRING").option("sep", " ").csv(
    find_absolute_path("Marvel_Names.csv"))

df_connection = spark.read.text(find_absolute_path("Marvel_Graph.txt"))
df_hero_connections = df_connection.withColumn("id", F.split(df_connection.value, " ")[0].cast("int"))\
    .withColumn("connections", F.size(F.split(df_connection.value, " ")) - 1)\
    .groupBy("id")\
    .agg(F.sum("connections").alias("connections"))

min_max = df_hero_connections.agg(F.min("connections").alias("min_connections"), F.max("connections").alias("max_connections"))\
    .first()
df_obscure_heros = df_hero_connections.filter(df_hero_connections.connections == min_max.min_connections)\
    .join(df_marvel_names, "id", how="left")

df_obscure_heros.select("id", "name")\
    .orderBy(F.asc("id"))\
    .show(df_obscure_heros.count(), truncate=False)

spark.stop()
