from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# creating lines RDD
lines = sc.textFile("file:///SparkCourse/ml-latest-small/ratings.csv")

# extract the data we care and assign to new ratings RDD
ratings = lines.map(lambda x: x.split(',')[2])

# perfor an action countByValue and convert RDD into the form tuples (key, value)
result = ratings.countByValue()
print("result: ", result)

# using collection package to create OrderedDict
sortedResults = collections.OrderedDict(sorted(result.items()))
print("sortedResults: ", sortedResults)

for key, value in sortedResults.items():
    print('{} {}'.format(key, value))