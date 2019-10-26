import pyspark as ps

conf = ps.SparkConf().setMaster("local[4]").setAppName("actions")
sc = ps.SparkContext(conf=conf)

# take(n): Returns n elements from a RDD
# takeSample(): More suitable for taking a sample
# takeOrdered(), top(n): For ordered return

rdd = sc.parallelize([5, 1, 3, 2])
rdd.takeOrdered(4)
# Output: [1, 2, 3, 4]

rdd.takeOrdered(4, lambda n: -n)
# Output: [5, 3, 2, 1]


# reduce(): Take two elements of the same type and returns one new element
rdd = sc.parallelize([1, 2, 3])
rdd.reduce(lambda x, y: x * y)
# Output: 6


# cache() or persist(MEMORY_ONLY): cache a RDD in order to not reading a file twice

