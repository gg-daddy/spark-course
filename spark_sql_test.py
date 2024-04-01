from pyspark.sql import SparkSession, Row
from dataset.utils import find_absolute_path

spark = SparkSession.builder.appName("pysql_test").getOrCreate()
lines = spark.sparkContext.textFile(find_absolute_path("fakefriends.csv"))


def parse_line(line):
    fields = line.split(',')
    return Row(
        ID=int(fields[0]),
        name=str(fields[1].encode("utf-8")),
        age=int(fields[2]),
        numFriends=int(fields[3])
    )


scheme_person = lines.map(parse_line).toDF().cache()
scheme_person.createOrReplaceTempView("people")

result = spark.sql(
    "SELECT age, sum(numFriends) sum_of_fridends, avg(numFriends)  avg_of_fridends \
        FROM people WHERE age >= 13 and age <= 50 \
        group by age order by sum_of_fridends desc") \
    .collect()
for row in result:
    print(row)

scheme_person.groupBy("age").count().orderBy("age").show()
spark.stop()
