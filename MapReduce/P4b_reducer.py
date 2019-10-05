import sys
import re

moviesList = ["", "", "", "", ""]

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    avgRating, idMovie = re.split("\t", line)

    if float(avgRating) <= 1:
        moviesList[0] += idMovie + ", "
    elif 1 < float(avgRating) <= 2:
        moviesList[1] += idMovie + ", "
    elif 2 < float(avgRating) <= 3:
        moviesList[2] += idMovie + ", "
    elif 3 < float(avgRating) <= 4:
        moviesList[3] += idMovie + ", "
    else:
        moviesList[4] += idMovie + ", "


for i, rang in enumerate(moviesList):
    print("Rango {}: {}\n\n".format(i + 1, rang[:len(rang) - 2]))