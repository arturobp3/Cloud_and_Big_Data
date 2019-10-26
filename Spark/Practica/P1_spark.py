import sys
import pyspark as ps

word = sys.argv[1]

conf = ps.SparkConf().setMaster("local[4]").setAppName("p1_spark")
sc = ps.SparkContext(conf=conf)

textPath = 'input.txt'
textRDD = sc.textFile(textPath)

grep = textRDD\
    .filter(lambda line: word in line)\
    .collect()

print(grep)