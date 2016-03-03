from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopMovie")
sc = SparkContext(conf = conf)

def parseLines(text):
    fields = text.split()
    movieId = int(fields[1])
    return (movieId, 1)

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieId = int(fields[0])
            name = fields[1]
            movieNames[movieId] = name
    return movieNames

movieNamesDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("ml-100k/u.data")
total = lines.map(parseLines).reduceByKey(lambda x,y: x + y).map(lambda(x,y):(y,x)).sortByKey()
totalsWithNames = total.map(lambda (count, movieId) : (movieNamesDict.value[movieId], count))

results = totalsWithNames.collect()

for result in results:
    print "The  movie %s was watched %i times" % (result[0], result[1])
print "The top movie is %s and it was watched %i times" % (results[-1][0], results[-1][1])
