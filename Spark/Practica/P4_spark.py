import pyspark as ps
from pyspark.sql import SQLContext

conf = ps.SparkConf().setMaster("local[4]").setAppName("p4_spark")
sc = ps.SparkContext(conf=conf)


textPath = '../../Datasets/ratings.csv'
sqlContext = SQLContext(sc)
df = sqlContext.read.csv(textPath)

# Extraemos la columna movieId y rating
dataDF = df.select("_c1", "_c2")


header = dataDF.rdd.first()
dataRDD = dataDF.rdd.filter(lambda x: x != header)
dataRDD = dataRDD.map(lambda x: (int(x._c1.encode('utf-8').split("-")[0]), float(x._c2.encode('utf-8'))))


dataPerMovie = dataRDD.groupByKey().map(lambda x: (x[0], list(x[1])))
sumDataPerMovie = dataPerMovie.map(lambda x: (x[0], sum(x[1])))
elementsPerData = dataPerMovie.map(lambda x: (x[0], len(x[1])))

elementsJoined = sumDataPerMovie.join(elementsPerData)
averagePerMovie = elementsJoined.map(lambda x: (x[0], x[1][0]/x[1][1]))

rddAverage = averagePerMovie.sortByKey().collect()


moviesList = ["", "", "", "", ""]

for line in rddAverage:
    idMovie = line[0]
    avgRating = line[1]

    if float(avgRating) <= 1:
        moviesList[0] += str(idMovie) + ", "
    elif 1 < float(avgRating) <= 2:
        moviesList[1] += str(idMovie) + ", "
    elif 2 < float(avgRating) <= 3:
        moviesList[2] += str(idMovie) + ", "
    elif 3 < float(avgRating) <= 4:
        moviesList[3] += str(idMovie) + ", "
    else:
        moviesList[4] += str(idMovie) + ", "


for i, rang in enumerate(moviesList):
    print("Rango {}: {}\n\n".format(i + 1, rang[:len(rang) - 2]))