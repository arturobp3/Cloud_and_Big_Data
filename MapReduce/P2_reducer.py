import sys
import re

urlBefore = None
sum = 0

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    url, quantity = re.split("\t", line)

    if(urlBefore != url):
        if(urlBefore is not None):
            print(urlBefore + "\t" + str(sum))
        urlBefore = url
        sum = int(quantity)
    else:
        sum += int(quantity)

print(urlBefore + "\t" + str(sum))