from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors
from dataset.utils import find_absolute_path

spark = SparkSession.Builder().appName("LinearRegression").getOrCreate()
input_files = spark.sparkContext.textFile(find_absolute_path("regression.txt"))


def parse_line(line):
    fields = line.split(",")
    return (float(fields[0]), Vectors.dense(float(fields[1])))


df = input_files.map(parse_line).toDF(["label", "features"])
df_training, df_test = df.randomSplit([0.8, 0.2])

lr = LinearRegression(
    maxIter=10,
    regParam=0.3,
    elasticNetParam=0.8,
    fitIntercept=True
)
model = lr.fit(df_training)

predictions = model.transform(df_test)\
    .select("label", "prediction")\
    .collect()
for prediction in predictions:
    print(f"Label: {prediction[0]:.3f}, Predication: {prediction[1]:.3f}")

spark.stop()
