from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

def parseLines(text):
    fields = text.split()
    movieId = fields[1]
    return (movieId, 1)


lines = sc.textFile("ml-100k/u.data")
total = lines.map(parseLines).reduceByKey(lambda x,y: x + y).map(lambda(x,y):(y,x)).sortByKey()

results = total.collect()
print "The top movie is MovieID %s and it was watched %i times" % (results[-1][1], results[-1][0])
