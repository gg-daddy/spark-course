from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TemperatureSearch")
sc = SparkContext(conf=conf)


station_temp = sc.textFile(
    "/Users/chenyanbin/codebase/spark/spark-course/dataset/1800.csv")


def parse_line(line):
    line_split = line.split(",")
    return (line_split[0], float(line_split[3]) * 0.1)


result = station_temp.filter(lambda x: "TMAX" in x).map(
    parse_line).reduceByKey(lambda x, y: max(x, y)).collect()


def temp_f_from_c(temp_c):
    return temp_c * (9.0 / 5.0) + 32.0


for station, tem_c in result:
    print(f"{station},{tem_c:.2f}C,{temp_f_from_c(tem_c):.2f}F")
