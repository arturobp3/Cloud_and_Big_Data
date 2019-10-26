import pyspark as ps


conf = ps.SparkConf().setMaster("local").setAppName("example1")
sc = ps.SparkContext(conf=conf)

# RDD Creation
# ------------
logFile = '/var/log/syslog'
textRDD = sc.textFile(logFile)
print(textRDD.count())
linesWithRoot = textRDD.filter(lambda line: 'root' in line)
linesWithRoot.take(9)

# Other RDD
# ---------
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
distDataS = distData.map(lambda x: x * x, data)


