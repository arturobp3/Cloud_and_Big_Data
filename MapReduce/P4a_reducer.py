import sys
import re

previousId = None
sum = 0
cont = 0

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    idRead, rating = re.split("\t", line)

    if previousId != idRead:
        if previousId is not None:
            print(previousId + "\t" + str(sum/cont))
        previousId = idRead
        sum = float(rating)
        cont = 1
    else:
        sum += float(rating)
        cont += 1

print(previousId + "\t" + str(sum/cont))