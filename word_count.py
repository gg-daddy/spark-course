from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile(
    "/Users/chenyanbin/codebase/spark/spark-course/dataset/book.txt")

result = lines.flatMap(lambda x: re.split(r'\W+', x.lower())) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=True) \
    .collect()

for word, count in result:
    print(f"{word} : {count}")
