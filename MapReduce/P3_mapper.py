import sys
import re

header = sys.stdin.readline()

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    wordsList = re.split(",", line)
    date = wordsList[0]
    close = wordsList[4]

    print(date + "\t" + close)