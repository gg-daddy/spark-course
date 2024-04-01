from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from dataset.utils import find_absolute_path

spark = SparkSession.Builder().appName("CustomerSpendSummary").getOrCreate()

scheme = StructType().add("userId", StringType(), True) \
    .add("itemId", StringType(), True) \
    .add("spent", FloatType(), True)

df = spark.read.schema(scheme).csv(find_absolute_path("customer-orders.csv"))
result = df.select("userId", "spent")\
    .groupBy("userId")\
    .agg(F.round(F.sum("spent"), 2).alias("total_spent"))\
    .sort("total_spent").collect()
# collect will return a list of Row objects and send it to the driver.
for each in result:
    print(f"{each[0]:<3} -> {each[1]:.2f}")

spark.stop()
