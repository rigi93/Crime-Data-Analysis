from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	def toCSVLine(data):
		return ','.join(str(d) for d in data)
# Useful to define how CSV is stored
	text_file = sc.textFile(sys.argv[1], 1)
# Extract text file from final.csv in same directory
	text_file = text_file.mapPartitions(lambda x: reader(x))
# Read contents from CSV
	text_file = text_file.map(lambda x: x[4])
# Keycode is the 5th column therefore n-1=4 hence map x[4]
	head = text_file.first()
# Store the first line of CSV. It will contain header. In the next statement do not perform operations on header
	cnts = text_file.filter(lambda x: x != head).map(lambda x: (x,1)).reduceByKey(add).sortByKey()
# maps keycode category to 1 then adds all the ones with same keycodes and sorts it
	keycode = cnts.map(toCSVLine)
# Map the output in CSV predefined format
	keycode.saveAsTextFile('keycode.csv')
# Save the output as a CSV file
