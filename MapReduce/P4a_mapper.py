import sys
import re

header = sys.stdin.readline()

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    wordsList = re.split(",", line)
    ids = wordsList[1]
    rating = wordsList[2]

    print(ids + "\t" + rating)