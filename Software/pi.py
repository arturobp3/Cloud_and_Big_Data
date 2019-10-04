from pyspark import SparkConf, SparkContext
import string

conf = SparkConf().setMaster('local').setAppName('Pi')
sc = SparkContext(conf = conf)

N = 10000000
delta_x = 1.0 / N
print sc.parallelize( xrange (N) ).map( lambda i: (i +0.5) * delta_x ).map( lambda x: 4 / (1 + x **2) ).reduce ( lambda a, b: a+b) * delta_x
