from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from dataset.utils import find_absolute_path

spark = SparkSession.Builder().appName("MinTemperature").getOrCreate()

scheme = StructType().add("stationID", StringType(), True) \
    .add("date", StringType(), True) \
    .add("measure_type", StringType(), True) \
    .add("temperature", FloatType(), True)

df = spark.read.schema(scheme).csv(find_absolute_path("1800.csv"))

min_temp_c = df.filter(df.measure_type == 'TMIN')\
    .select("stationID", "temperature")\
    .groupBy("stationID")\
    .agg(F.min("temperature").alias("min_temperature_c"))

result = min_temp_c.withColumn(
    "min_temperature_f",
    F.round(F.col("min_temperature_c") * 0.1 * (9.0 / 5.0) + 32, 2)
).select("stationID", "min_temperature_f").collect()

for each in result:
    print(f"{each.stationID}, {each.min_temperature_f:.2f}F")
spark.stop()
