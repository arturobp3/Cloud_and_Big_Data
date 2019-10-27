import pyspark as ps
from pyspark.sql import SQLContext

conf = ps.SparkConf().setMaster("local[4]").setAppName("p5_spark")
sc = ps.SparkContext(conf=conf)


textPath = 'Meteorite_Landings.csv'
sqlContext = SQLContext(sc)
df = sqlContext.read.csv(textPath)

# Extraemos la columna nameType y Mass
dataDF = df.select("_c2", "_c4")

NoneType = type(None)
dataRDD = dataDF.rdd.filter(lambda x: type(x._c2) != NoneType and type(x._c4) != NoneType)
dataRDD = dataRDD.map(lambda x: (x._c2.encode('utf-8').split("-")[0], float(x._c4.encode('utf-8'))))

dataPerType = dataRDD.groupByKey().map(lambda x: (x[0], list(x[1])))
sumDataPerType = dataPerType.map(lambda x: (x[0], sum(x[1])))
elementsPerData = dataPerType.map(lambda x: (x[0], len(x[1])))

elementsJoined = sumDataPerType.join(elementsPerData)
averagePerType = elementsJoined.map(lambda x: (x[0], x[1][0]/x[1][1]))

print(averagePerType.sortByKey().collect())
