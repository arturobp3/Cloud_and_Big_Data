from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("example1")
sc = SparkContext(conf=conf)

# RDD Creation
# ------------
logFile = '/var/log/syslog'
textRDD = sc.textFile(logFile) # create RDD
print(textRDD.count())
linesWithRoot = textRDD.filter(lambda line: 'root' in line)
print(linesWithRoot.take(9))
