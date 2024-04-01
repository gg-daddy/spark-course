from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark DataFrame Directly").getOrCreate()
