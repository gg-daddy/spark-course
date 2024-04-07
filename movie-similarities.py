from pyspark.sql import SparkSession, functions as F
from dataset.utils import find_absolute_path
import sys


def compute_consine_similarity(movie_rating: F.DataFrame):
    '''
    下面的代码是计算余弦相似度的代码，这里的余弦相似度是指两个向量的夹角余弦值，值越大表示两个向量越相似。
    余弦相似度的计算公式如下：
    cos(θ) = A·B / |A|·|B|
    其中A·B表示向量A和向量B的点积，|A|表示向量A的模，|B|表示向量B的模。
    余弦相似度的取值范围是[-1, 1]，当余弦相似度为1时，表示两个向量的方向完全相同，余弦相似度为-1时，表示两个向量的方向完全相反。
    余弦相似度为0时，表示两个向量是正交的。
    余弦相似度的计算公式可以转化为下面的形式：
    cos(θ) = Σ(Ai * Bi) / sqrt(Σ(Ai^2)) * sqrt(Σ(Bi^2))。
    这里的Ai和Bi分别表示向量A和向量B的第i个元素。 

    对应到当前的场景， Ai 和 Bi 分别代表同一个人分别对两部电影的评分。 
    一个电影对应的向量就是所有用户对这部电影的评分，向量的每个元素是一个用户对这部电影的评分。
    '''
    pair_scores = movie_rating.withColumn("xx", F.col("rating1") * F.col("rating1"))\
        .withColumn("yy", F.col("rating2") * F.col("rating2"))\
        .withColumn("xy", F.col("rating1") * F.col("rating2"))

    caculated_similarity_scores = pair_scores.groupBy("movie_id1", "movie_id2")\
        .agg(
            F.sum(F.col("xy")).alias("numerator"),
            (F.sqrt(F.sum(F.col("xx"))) *
             F.sqrt(F.sum(F.col("yy")))).alias("denominator"),
            F.count(F.col("xy")).alias("num_pairs"),
    )
    result = caculated_similarity_scores.withColumn(
        "score", F.when(F.col("denominator") != 0, F.col("numerator") / F.col("denominator")).otherwise(0))\
        .select("movie_id1", "movie_id2", "score", "num_pairs")

    return result


def get_movie_name(df_movie_names: F.DataFrame, movie_id: int):
    return df_movie_names.filter(F.col("movie_id") == movie_id).select("movie_name").first().movie_name


spark = SparkSession.Builder().appName(
    "MovieSimilarities").master("local[*]").getOrCreate()


df_movie_ratings = spark.read.option("sep", "\t").schema(
    "user_id int, movie_id int, rating int, timestamp int").csv(find_absolute_path("u.data"))

# self join to get all possible pairs of movie ratings by the same user
df_self_joined_movie_ratings = df_movie_ratings.alias("rating1").filter(F.col("rating1.rating") > 3)\
    .join(df_movie_ratings.alias("rating2").filter(F.col("rating2.rating") > 3),
          (F.col("rating1.user_id") == F.col("rating2.user_id")) &
          (F.col("rating1.movie_id") < F.col("rating2.movie_id")))\
    .select(
        F.col("rating1.movie_id").alias("movie_id1"),
        F.col("rating2.movie_id").alias("movie_id2"),
        F.col("rating1.rating").alias("rating1"),
        F.col("rating2.rating").alias("rating2"))\



df_consine_similarity = compute_consine_similarity(
    df_self_joined_movie_ratings).cache()

if len(sys.argv) > 1:
    target_movie_id = int(sys.argv[1])
    score_threshold = 0.97
    co_occurrence_threshold = 50

    hit_movie_result = df_consine_similarity.filter((F.col("movie_id1") == target_movie_id) | (F.col("movie_id2") == target_movie_id))\
        .filter(F.col("score") >= score_threshold)\
        .filter(F.col("num_pairs") >= co_occurrence_threshold).sort(F.col("score").desc())\
        .take(10)
    df_movie_names = spark.read.option("sep", "|").schema(
        "movie_id int, movie_name string").csv(find_absolute_path("u.item"))

    print(
        f"Found top 10 matched movies for {get_movie_name(df_movie_names, target_movie_id)}:")
    print("=" * 50)
    for index, row in enumerate(hit_movie_result):
        matched_movie_id = row.movie_id2 if row.movie_id1 == target_movie_id else row.movie_id1
        print(
            f"{index + 1:>2}.{get_movie_name(df_movie_names, matched_movie_id):<55}|{row.score:.3f}|{row.num_pairs:>4}")

spark.stop()
