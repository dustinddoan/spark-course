from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# extract only (age, numFriends) as (key, value)
def parseLine(line):
  fields = line.split(',')
  age = int(fields[2])
  numFriends = int(fields[3])
  return (age, numFriends)

# reading source data
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")

# transform input data into (key, value) RDD each line
rdd = lines.map(parseLine)

# mapValues - leave the key untouch (key=age remain, only numFriends pass into the function)
# reducebyKey - combine value of the same key
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# mapValues
averageByAge = totalsByAge.mapValues(lambda x: x[0] / x[1]).sortByKey(ascending=True)
print('ave', averageByAge)

results = averageByAge.collect()

for result in results:
  print(result)



