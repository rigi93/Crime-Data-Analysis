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
	
	def basetype_string(input):
		try:
			if type(input) is str:
				return "STRING"
		except ValueError:
			return type(input)		

	def basetype_int(input):
		try:
			number = int(input)
			return "INT"
		except ValueError:
			return type(input)
	
	def basetype_date(input):
		mat=re.match('(\d{2}|\d{1})/(\d{2}|\d{1})/(\d{4})$', input)
		if mat is not None:
			return "Date"
		else:
			return type(input)
		
	def semantictype_date(value):
		mat=re.match('(\d{2}|0?[1-9])/(\d{2}|0?[1-9])/(\d{4})$', value)
		if mat is not None:
			return "Complaint Date"
		else:
			return "Other"

	def semantictype_boro(value):
		try: 
			if (len(value) > 4 and len(value) < 14 and type(value) == str):
				return "Borough name"
			else:
				return "Other"
		except ValueError:	
				return "Other"	
				
	def semantictype_status(value):
		try: 
			if (len(value) > 0 and len(value) < 10 and type(value) == str):
				return "Crime Status"
			else:
				return "Other"
		except ValueError:	
				return "Other"

	def semantictype_crimetype(value):
		try: 		
			if (len(value) > 5 and len(value) < 12 and type(value) == str):
				return "Crime Type"
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
	
	def validity_complaint_to(x):
		if x == '':
			return "not defined"
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
	
	def validity_time(x):
		if x == '':
			return "NULL"
		try:
			if type(x) is not datetime.date:
 				return "VALID"
 			else:
 				return "INVALID"
		except ValueError:
			return "INVALID";
			
	def validity_boro(x):
		x = x.upper()
		boroughs = ['BROOKLYN', 'STATEN ISLAND', 'MANHATTAN', 'QUEENS', 'BRONX']
		if x == '':
			return "NULL"
		elif x in boroughs:
			return "VALID"
		else:	
			return "INVALID"

			
	def validity_location(x):
		x = x.upper()
		locations = ['INSIDE', 'OUTSIDE', 'FRONT OF', 'REAR OF', 'OPPOSITE OF']
		if x == '':
			return "NULL"
		elif x in locations:
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
			
	def validity_code(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 999):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"
		
	def validity_parks(x):
		if x == '':
			return "Not a park"
		else:
			return "VALID"
		
	def validity_hadevelopt(x):
		if x == '':
			return "Not a hadevelopt"
		else:
			return "VALID"
	
	def validity_xcoord(x):
		if x == '':
			return "NULL"
		else:
			return "VALID"
	
	def validity_ycoord(x):
		if x == '':
			return "NULL"
		else:
			return "VALID"
				
	def validity_complaint_number(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) == 9 and x.isdigit() and int(x) > 100000000 and int(x) < 999999999):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"

	def validity_latitude(x):
		try: 
			if x == '':
				return "NULL"
			elif (x.isdigit() and int(x) > -90 and int(x) < 90):
				return "INVALID"
			else:	
				return "VALID"
		except ValueError:
			return "INVALID"

	def validity_longitude(x):
		try: 
			if x == '':
				return "NULL"
			elif (x.isdigit() and int(x) > -180 and int(x) < 180):
				return "INVALID"
			else:	
				return "VALID"
		except ValueError:
			return "INVALID"

	def toCSVLine(data):
	  return ','.join(str(d) for d in data)			
	
	deliverable = lines.map(lambda x: (x[0], basetype_string(x[0]), validity_complaint_number(x[0]),\
						x[1], basetype_date(x[1]), semantictype_date(x[1]), validity_date(x[1]),\
						x[2], validity_time(x[2]),\
						x[3], basetype_date(x[3]), semantictype_date(x[3]), validity_complaint_to(x[3]),\
						x[4], validity_time(x[4]),\
						x[5], basetype_date(x[5]), semantictype_date(x[5]), validity_date(x[5]),\
						x[6], basetype_int(x[6]), semantictype_cd(x[6]), validity_code(x[6]),\
						x[7], basetype_string(x[7]),\
						x[8], basetype_int(x[8]), semantictype_cd(x[8]), validity_code(x[8]),\
						x[9], basetype_string(x[9]),\
						x[10], semantictype_status(x[10]), validity_crime(x[10]),\
						x[11], basetype_string(x[11]), semantictype_crimetype(x[11]), validity_law_category(x[11]),\
						x[12], basetype_string(x[12]),\
						x[13], basetype_string(x[13]), semantictype_boro(x[13]), validity_boro(x[13]),\
						x[14], basetype_int(x[14]),\
						x[15], basetype_string(x[15]), validity_location(x[15]),\
						x[16], basetype_string(x[16]),\
						x[17], basetype_string(x[17]),\
						x[18], basetype_string(x[18]),\
						x[19], basetype_string(x[19]), validity_xcoord(x[19]),\
						x[20], basetype_string(x[20]), validity_ycoord(x[20]),\
						x[21], basetype_string(x[21]), validity_latitude(x[21]),\
						x[22], basetype_string(x[22]), validity_longitude(x[22]),\
						x[23], basetype_string(x[23])))

	deliverable = deliverable.filter(lambda x: x[2] == "VALID" and x[6] == "VALID" and x[8] == "VALID" \
									and (x[12] == "VALID" or x[12] =="not defined") and x[14] == "VALID" and x[18] == "VALID" \
									and x[22] == "VALID" and x[28] == "VALID" and x[33] == "VALID" \
									and x[37] == "VALID" and x[43] == "VALID" and x[48] == "VALID" \
									and x[57] == "VALID" and x[60] == "VALID" and x[63] == "VALID" \
									and x[66] == "VALID") \
				.map(lambda x: (x[0], x[3], x[7], x[9], x[13], x[15], x[19], x[23], x[25], x[29], x[31], x[34], x[38], x[40], x[44], x[46], x[49], x[51], x[53], x[55], x[58], x[61], x[64], x[67]))

	deliverable = deliverable.map(toCSVLine)
	deliverable.saveAsTextFile("final.csv")	
	sc.stop()
