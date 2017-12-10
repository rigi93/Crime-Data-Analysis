from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime, date
import pandas

def formatDate(input):
    try:
        split = input.map(lambda line: line.split("/")).collect()
        return split
    except:
		return "999"

def getDay(input):
	try:
		input = formatDate(input)
		return input.map(lambda x:x[1] if x[1] != "" else x[3])
	except Exception:
		return "Other"


if __name__ == "__main__":
  sc = SparkContext()
  lines = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
	header = lines.first()
	lines = lines.filter(lambda x: x!=header).map(lambda x: x[0])
	crime_date = getDay(lines)
	crime_date_count = sorted(crime_date.flatMap(lambda line: line.split()).map(lambda w: (w,1)).reduceByKey(lambda v1, v2: v1+v2).collect())
	crime_date_count = crime_date_count.map(toCSVLine)
	deliverable.saveAsTextFile(“DataByDate.csv")	
	sc.stop()
