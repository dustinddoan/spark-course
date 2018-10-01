from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
  fields = line.split(',')
  
  stationID = fields[0]
  entryType = fields[2]
  temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32

  return (stationID, entryType, temperature)
  # (ITE00100554,TMAX,-75)


lines = sc.textFile("file:///SparkCourse/data/1800.csv")
parsedLines = lines.map(parseLine)

# filter out TMAX
maxTemps = parsedLines.filter(lambda x: 'TMAX' in x[1])

# return (ITE00100554, -75)
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

# find min temp of the same stationID
# (ITE00100554, -125)
# (ITE00100554, -13)

maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()

for result in results:
  print(result[0] + "\t{:.2f}F".format(result[1]))