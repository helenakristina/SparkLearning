from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AmountByCustomer")
sc = SparkContext(conf = conf)

def parseLine(text):
    line = text.split(",")
    custId = int(line[0])
    amount = float(line[2])
    return (custId, amount)

#create rdd from textfile
lines = sc.textFile("customer-orders.csv")

sales = lines.map(parseLine).reduceByKey(lambda x,y: x + y).map(lambda (x,y):(y,x)).sortByKey()
results = sales.collect()
for result in results:
    print str(result[1]) + "\t${:.2f}".format(result[0])
