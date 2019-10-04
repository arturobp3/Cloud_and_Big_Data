import sys
import re

word = sys.argv[1]

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    wordsList = re.split(r"\W+", line)

    if(word in wordsList):
        print(line)


