from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime, date
import pandas

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	lines = lines.mapPartitions(lambda x: reader(x))
	header = lines.first()
	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[3], x[7], x[23]))
	
	def basetype_string(input):
		try:
			if type(input) is str:
				return "STRING"
		except ValueError:
			return type(input)		

	def validity_boro(x):
		if x == '':
			return "NULL"
		x = x.upper()
		boroughs = ['BROOKLYN', 'STATEN ISLAND', 'MANHATTAN', 'QUEENS', 'BRONX']
		if x in boroughs:
			return "VALID"
		else:	
			return "INVALID"

	def validity_location(x):
		if x == '':
			return "NULL"
		else:	
			return "VALID"
			
	def validity_agency(x):
		if x == '':
			return "NULL"
		x = x.upper()
		agency = ['NYPD', 'HPD', 'DOT', 'DCA', 'DOHMH', 'DEP', 'DPR', 'DSNY', 'DOB', 'DHS', 'DOITT', 'CHALL', 'TLC', 'HRA']
		if x in agency:
			return "VALID"
		else:	
			return "INVALID"
			
	def toCSVLine(data):
	  return ','.join(str(d) for d in data)			
	
	deliverable = lines.map(lambda x: (x[0], basetype_string(x[0]), validity_agency(x[0]),\
						x[1], basetype_string(x[1]), validity_location(x[1]),\
						x[2], basetype_string(x[2]), validity_boro(x[2])))

	deliverable = deliverable.filter(lambda x: x[1] == "STRING" and x[2] == "VALID" and x[4] == "STRING" and\
							x[5] == "VALID" and x[7] == "STRING" and x[8] == "VALID") \
							.map(lambda x: (x[0], x[3], x[6]))

	deliverable = deliverable.map(toCSVLine)
	deliverable.saveAsTextFile("cleanedComplainData.csv")	
	sc.stop()