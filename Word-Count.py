from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalize(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("Book.txt")
words = input.flatMap(normalize)
'''
#alternative method using collections and sorting results
import collections

wordCounts = words.countByValue()

sortedResults = collections.OrderedDict(sorted(wordCounts.items(), key = lambda k: k[1]))
for word, count in sortedResults.iteritems():
    cleanWord = word.encode('ascii','ignore')
    if (cleanWord):
        print cleanWord, count
'''

#better method to use RDDs to keep this scalable using map and reduce functions
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda (x,y): (y,x)).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print word + ":\t\t" + count
