from pyspark.sql import SparkSession
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

from dataset.utils import find_absolute_path

spark = SparkSession.Builder().appName("DecisionTree").getOrCreate()
df = spark.read.option("header", "true")\
    .option("inferSchema", "true")\
    .csv(find_absolute_path("realestate.csv"))

assembler = VectorAssembler(
    inputCols=[
        "HouseAge",
        "DistanceToMRT",
        "NumberConvenienceStores",
        "Latitude",
        "Longitude"
    ],
    outputCol="features"
)
df = assembler.transform(df)
df_training, df_test = df.randomSplit([0.5, 0.5])

dt = DecisionTreeRegressor(featuresCol="features", labelCol="PriceOfUnitArea")
model = dt.fit(df_training)
print(model.toDebugString)

# 创建一个RegressionEvaluator对象
evaluator = RegressionEvaluator(
    labelCol="PriceOfUnitArea",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(model.transform(df_test))
print(f"Root Mean Squared Error (RMSE) on test data = {rmse:.3f}")

spark.stop()
