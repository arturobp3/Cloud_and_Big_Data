import sys
import re

previousType = None
sum = 0
cont = 0

for line in sys.stdin:
    line = re.sub( r'^\W+|\W+$', '', line )
    typeM, mass = re.split("\t", line)

    if previousType != typeM:
        if previousType is not None:
            print(previousType + "\t" + str(sum/cont))
        previousType = typeM
        sum = float(mass)
        cont = 1
    else:
        sum += float(mass)
        cont += 1

print(previousType + "\t" + str(sum/cont))