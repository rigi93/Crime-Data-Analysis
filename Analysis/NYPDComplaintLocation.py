from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader

def toCSVLine(data):
        return ','.join(str(d) for d in data)
	
def assignLocation(data):	
		x = x.upper()
		house = ['RESIDENTIAL BUILDING', 'HOUSE']
		commercial = ['COMMERCIAL', 'RESTAURANTS', 'STORE', 'DELI', 'BAR']
		street = ['STREET', 'SIDEWALK', 'CURB']
		if any(x in str for x in house):
			return "RESIDENTIAL"
		elif if any(x in str for x in commercial):
			return "COMMERCIAL"
		elif if any(x in str for x in street):
			return "OUTDOORS/ROADSIDE"
		else:	
			return "OTHER"
		

if __name__ == "__main__":
        sc = SparkContext()
        lines = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        header = lines.first()
        lines = lines.filter(lambda x: x!=header).map(lambda x: (x[3], x[7]))
		lines = lines.filter(lambda x: x[0] == "NYPD").map(lambda x: x[1])
        location = lines.map(lambda x: assignLocation(x)).map(lambda w: (w,1)).reduceByKey(add).sortByKey()
        location = location.map(toCSVLine)
        location.saveAsTextFile("ComplaintByLocation.csv")
        sc.stop()
