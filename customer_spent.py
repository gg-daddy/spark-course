from pyspark import SparkConf, SparkContext
from dataset.utils import find_absolute_path

conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf=conf)

orders = sc.textFile(find_absolute_path("customer-orders.csv"))


def parse_line(order_line):
    customer_id, _, spent = order_line.split(',')
    return int(customer_id), float(spent)


result = orders.map(parse_line) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False) \
    .collect()

for customer_id, spent in result:
    print(f"{customer_id:<3} -> {spent:.2f}")
