import sys
import re

previousUrl = None
sum = 0

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    url, quantity = re.split("\t", line)

    if previousUrl != url:
        if previousUrl is not None:
            print(previousUrl + "\t" + str(sum))
        previousUrl = url
        sum = int(quantity)
    else:
        sum += int(quantity)

print(previousUrl + "\t" + str(sum))