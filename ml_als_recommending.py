from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from dataset.utils import find_absolute_path


def load_movie_names():
    movie_names = {}
    with open(find_absolute_path("u.item"), encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split("|")
            movie_names[int(fields[0])] = fields[1]
    return movie_names


movie_names = load_movie_names()

spark = SparkSession.Builder().appName(
    "Most Rating Using Join!").getOrCreate()

df_movie_rating = spark.read.option("sep", "\t")\
    .schema("userId INT, movieId INT, rating INT, ts INT")\
    .csv(find_absolute_path("u.data"))

als = ALS().setMaxIter(10)\
    .setRegParam(0.01)\
    .setUserCol("userId")\
    .setItemCol("movieId")\
    .setRatingCol("rating")

model = als.fit(df_movie_rating)

# 假设我们有一个 DataFrame，其中包含用户的 ID
df_users = spark.createDataFrame([(1,), (2,)], ["userId"])

# 使用训练好的模型为这些用户进行推荐
recommendations = model.recommendForUserSubset(
    df_users, numItems=10).collect()

for recommendation in recommendations:
    print(f"Recommendations for user {recommendation.userId}:")
    print("-" * 50)
    for movie in recommendation.recommendations:
        print(f"{movie_names[movie.movieId]:<30}, score: {movie.rating:.3f}")
    print("-" * 50)

spark.stop()
