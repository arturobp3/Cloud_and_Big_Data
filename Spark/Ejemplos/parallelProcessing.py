import pyspark as ps


conf = ps.SparkConf().setMaster("local").setAppName("parallelProcessing")
sc = ps.SparkContext(conf=conf)


data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data) # Create a distributed collection, Create a RDD (1 partition)
distDataP = sc.parallelize(data, 3) # Slice the dataset into 3 partitions, 3 way parallelism

print(distDataP.count())
print(distDataP.getNumPartitions())
print(distDataP.reduce(lambda x, y: x + y))