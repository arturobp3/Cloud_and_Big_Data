import pyspark as ps

conf = ps.SparkConf().setMaster("local[4]").setAppName("parallelExecution")
sc = ps.SparkContext(conf=conf)


N = 10
delta_x = 1.0 / N
data = sc.parallelize(range(N))
rdd = data.map(lambda i: (i + 0.5) * delta_x).map(lambda x: 4 / (1 + x **2) )
partitions = rdd.getNumPartitions()
print(rdd.reduce(lambda a, b: a + b) * delta_x)
print(partitions)
print(rdd.glom().collect())

