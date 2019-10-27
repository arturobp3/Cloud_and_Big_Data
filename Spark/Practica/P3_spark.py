import pyspark as ps
from pyspark.sql import SQLContext

conf = ps.SparkConf().setMaster("local[4]").setAppName("p3_spark")
sc = ps.SparkContext(conf=conf)


textPath = '../../Datasets/GOOGLE.csv'
sqlContext = SQLContext(sc)
df = sqlContext.read.csv(textPath)

# Extraemos la columna Dates y Close
dataDF = df.select("_c0", "_c4")


header = dataDF.rdd.first()
dataRDD = dataDF.rdd.filter(lambda x: x != header)
dataRDD = dataRDD.map(lambda x: (int(x._c0.encode('utf-8').split("-")[0]), float(x._c4.encode('utf-8'))))


dataPerYear = dataRDD.groupByKey().map(lambda x: (x[0], list(x[1])))
sumDataPerYear = dataPerYear.map(lambda x: (x[0], sum(x[1])))
elementsPerData = dataPerYear.map(lambda x: (x[0], len(x[1])))

elementsJoined = sumDataPerYear.join(elementsPerData)
averagePerYear = elementsJoined.map(lambda x: (x[0], x[1][0]/x[1][1]))

print(averagePerYear.sortByKey().collect())