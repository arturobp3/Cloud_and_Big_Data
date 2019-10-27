import pyspark as ps

conf = ps.SparkConf().setMaster("local[4]").setAppName("p2_spark")
sc = ps.SparkContext(conf=conf)


textPath = '../../Datasets/access_log'

textRDD = sc.textFile(textPath)

rows = textRDD.map(lambda line: (line.split(" ")[0], 1))

urlsFrecuency = rows.reduceByKey(lambda x, y: x + y)

print(urlsFrecuency.collect())
