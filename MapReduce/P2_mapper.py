import sys
import re

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    wordsList = re.split(" ", line)
    url = wordsList[0]

    print(url + "\t" + "1")