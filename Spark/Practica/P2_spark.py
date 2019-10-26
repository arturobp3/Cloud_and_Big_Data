import pyspark as ps

conf = ps.SparkConf().setMaster("local[4]").setAppName("p2_spark")
sc = ps.SparkContext(conf=conf)


textPath = '../../Datasets/access.log'
textRDD = sc.textFile(textPath)

