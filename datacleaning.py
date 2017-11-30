from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	
	lines = lines.mapPartitions(lambda x: reader(x))
	
	header = lines.take(1)

	lines = lines.filter(lambda x: x!=header).map(lambda x: (x[0], x[1], x[2], x[5], x[6], x[9], x[10], x[12]))
	
	def basetype_date(input):
		if mat is not None:
			return "DATETIME"
		else:
			return type(input)

	def basetype_string(input):
		try:
			if type(input) == str:
				return "STRING"
		except ValueError:
			return type(input)		

	def basetype_int(input):
		try:
			number = int(input)
			return "INT"
		except ValueError:
			return type(input)
			
	def semantictype_date(value):
		mat=re.match('(\d{2}|0?[1-9])/(\d{2}|0?[1-9])/(\d{4})$', value)
		if mat is not None:
			return "Complaint Date"
		else:
			return "Other"

	def semantictype_boro(value):
		try: 
			name = value.upper()		
			if (len(name) > 4 and len(name) < 14 and type(name) == str):
				return "Boro name"
			else:
				return "Other"
		except ValueError:	
				return "Other"	
				
	def semantictype_status(value):
		try: 
			name = value.upper()		
			if (len(name) > 0 and len(name) < 10 and type(name) == str):
				return "crime status"
			else:
				return "Other"
		except ValueError:	
				return "Other"

	def semantictype_crimetype(value):
		try: 
			name = value.upper()		
			if (len(name) > 5 and len(name) < 12 and type(name) == str):
				return "crime type"
			else:
				return "Other"
		except ValueError:	
				return "Other"				
 
	def semantictype_cd(x):
		try: 
			if (len(x) == 3) and x.isdigit():
				return "Offense Key Code"
			else:
				return "Other"
		except ValueError:	
				return "Other"

	def validity_date(x):
		if x == '':
			return "NULL"
		try:
			if x != datetime.strptime(x, "%m/%d/%Y").strftime('%m/%d/%Y'):
				raise ValueError
			mat=re.match('(1[0-2]|0?[1-9])/(3[01]|[12][0-9]|0?[1-9])/(20)(0[6-9]|1[0-5])$', x)
			if mat is not None:
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"		
		
	def validity_time(input):
		if isinstance(input, datetime.datetime):
			return "VALID"
		else:
			return "INVALID"
			
	def validity_boro(x):
		x = x.upper()
		boroughs = ['BROOKLYN', 'STATEN ISLAND', 'MANHATTAN', 'QUEENS', 'BRONX']
		if x == '':
			return "NULL"
		elif x in boroughs:
			return "VALID"
		else:	
			return "INVALID"

	def validity_crime(x):
		x = x.upper()
		status = ['ATTEMPTED', 'COMPLETED']
		if x == '':
			return "NULL"
		elif x in status:
			return "VALID"
		else:	
			return "INVALID"
			
	def validity_law_category(x):
		x = x.upper()
		category = ['FELONY', 'MISDEMEANOR', 'VIOLATION']
		if x == '':
			return "NULL"
		elif x in category:
			return "VALID"
		else:	
			return "INVALID"
			
	def validity_key_cd(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 900):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"
	
	def validity_complaint_number(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 900):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"

	def toCSVLine(data):
	  return ','.join(str(d) for d in data)			
	
	deliverable = lines.map(lambda x: (x[0], basetype_int(x[0]), validity_complaint_number(x[0]),\
						x[1], basetype_date(x[1]), semantictype_date(x[1]), validity_date(x[1]),\
						x[2], validity_time(x[2]),\
						x[3], basetype_date(x[3]), semantictype_date(x[3]), validity_date(x[3]),\
						x[4], basetype_int(x[4]), semantictype_cd(x[4]), validity_key_cd(x[4]),\
						x[5], basetype_string(x[5]), semantictype_status(x[5]), validity_crime(x[5]),\
						x[6], basetype_string(x[6]), semantictype_crimetype(x[6]), validity_law_category(x[6]),\
						x[7], basetype_string(x[7]), semantictype_boro(x[7]), validity_boro(x[7])))

	result = deliverable.filter(lambda x: x[2] == "VALID" and x[6] == "VALID" and x[8] == "VALID" \
								and x[12] == "VALID" and x[16] == "VALID" and x[20] == "VALID" \
								and x[24] == "VALID" and x[28] == "VALID") \
			.map(lambda x: (x[0], x[3], x[7], x[9], x[13], x[17], x[21], x[25]))
	
	cleaned = result.map(toCSVLine)
	
	cleaned.saveAsTextFile("cleaned_data.csv")
	
	sc.stop()