import sys
from csv import reader

for line in reader(sys.stdin):
    typeM = line[2]
    mass = line[4]

    if len(mass) != 0 and len(typeM) != 0:
        print(typeM + "\t" + mass)
