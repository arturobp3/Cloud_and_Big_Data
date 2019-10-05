import sys
import re

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    idMovie, ratingAverage = re.split("\t", line)

    print(ratingAverage + "\t" + idMovie)