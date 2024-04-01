from pyspark import SparkConf, SparkContext
from dataset.utils import find_absolute_path

conf = SparkConf().setMaster("local").setAppName("FridendsByAge - Average")
sc = SparkContext(conf=conf)

lines = sc.textFile(find_absolute_path("fakefriends.csv"))


def parse_line(line):
    age, num_friends = map(int, line.split(',')[2:4])
    return age, num_friends


friends_count = lines.map(parse_line)
avg_friends_by_age = friends_count.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (
    x[0] + y[0], x[1] + y[1])).mapValues(lambda x: x[0] / x[1]).collect()

for age, avg_friends in sorted(avg_friends_by_age, key=lambda x: x[1], reverse=True):
    print(f"{age},{avg_friends: .2f}")
