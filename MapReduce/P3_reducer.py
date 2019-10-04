import sys
import re

previousYear = None
sum = 0
cont = 0

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    date, closePrice = re.split("\t", line)
    year = re.split("-", date)[0]

    if previousYear != year:
        if previousYear is not None:
            print(previousYear + "\t" + str(sum/cont))
        previousYear = year
        sum = float(closePrice)
        cont = 1
    else:
        sum += float(closePrice)
        cont += 1

print(previousYear + "\t" + str(sum/cont))