from pyspark.sql import SparkSession, functions as F
from dataset.utils import find_absolute_path
import sys


def compute_consine_similarity(movie_rating: F.DataFrame):
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
