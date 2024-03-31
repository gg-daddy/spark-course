from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

'''
u.data     -- The full u data set, 100000 ratings by 943 users on 1682 items.
              Each user has rated at least 20 movies.  Users and items are
              numbered consecutively from 1.  The data is randomly
              ordered. This is a tab separated list of 
	         user id | item id | rating | timestamp. 
'''
lines = sc.textFile(
    "/Users/chenyanbin/codebase/spark/spark-course/dataset/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = sorted(result.items(), key=lambda x: x[1], reverse=True)
for key, value in sortedResults:
    print("%s %i" % (key, value))
