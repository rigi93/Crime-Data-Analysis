from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime, date

def toCSVLine(data):
        return ','.join(str(d) for d in data)

def getWeekday(input):
        return datetime.strptime(input, '%m/%d/%Y').weekday()

if __name__ == "__main__":
        sc = SparkContext()
        lines = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))
        header = lines.first()
        lines = lines.filter(lambda x: x!=header).map(lambda x: x[5])
        crime_date_count = lines.map(lambda x: getWeekday(x)).map(lambda w: (w,1)).reduceByKey(add).sortByKey()
        crime_date_count = crime_date_count.map(toCSVLine)
        crime_date_count.saveAsTextFile("DataByWeekday.csv")
        sc.stop()
