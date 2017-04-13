from collections import defaultdict
import datetime
import re
##########
#Some Global Variables
##########
date_format = '%m/%d/%Y'
time_format = '%H:%M:%S'
regex_3_digits = '^\d{3}$'

######Functions

def checkNull(string):
    #Assume Unicode String
    # Step 1: Check for length 0 i.e '' field
    if len(string)==0:
        return 'NULL'
    # Step 2: Check for 'nan'
    elif string=='nan':
        return 'NULL'
    else:
        return 'VALID'

def checkDate(line,date_format):
    try:
        date = datetime.datetime.strptime(line,date_format)
        if (date.year <= 2015) & (date.year >= 2006):
            return 'VALID'
        else:
            return 'INVALID/Year'
    except:
        return 'INVALID/FORMAT'
    
def checkTime(line,time_format):
    try:
        datetime.datetime.strptime(line,time_format)
        return 'VALID'
    except:
        return 'INVALID/Format'
    
def checkRegex(line,regex):
    # Input
    # line: a string for check
    # regex: regular expression pattern
    match = re.match(regex,line)
    if match:
        return 'VALID'
    else:
        return 'INVALID'


def checkCaseStatus(line):
    # check the validation of column offense descrption
    if checkNull(line) == 'VALID':
        if (line == 'COMPLETED' or line == 'ATTEMPTED'):
	    return 'VALID'
        else:
            return 'INVALID'
    else: 
        return 'NULL'

def checkLevelOfOffense(line):
    # check the validation of offense level
    if checkNull(line) == 'VALID':
        if line in {'FELONY', 'MISDEMEANOR', 'VIOLATION'}:
            return 'VALID'
        else: 
            return 'INVALID'
    else: 
        return 'NULL'

def checkBorough(line):
    # check the validation of borough
    borough = {'MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND'}
    if checkNull(line) == 'VALID':
        if line in borough:
            return 'VALID'
        else:    
            return 'INVALID'
    else: 
        return 'NULL'

def checkSpecificLoc(line):
    # check the validation of specific location
    spec_loc = {'FRONT OF', 'INSIDE', 'REAR OF', 'OUTSIDE', 'OPPOSITE OF'}
    if checkNull(line) == 'VALID':
        if line in spec_loc:
            return 'VALID'
        else:
            return 'INVALID'
    else:
        return 'NULL'

def checkNumber(line):
    try:
        number = float(line)
        return True
    except:
        return False

def checkLatitude(lat):
    # check the validation of latitude
    lat_range = [40.48, 40.9]
    if checkNull(lat) == 'VALID':
        if checkNumber(lat) and lat > lat_range[0] and lat < lat_range[1]:
            return 'VALID'
        else:
            return 'INVALID'
    else:
        return 'NULL'

def checkLongitude(lon):
    # check the validation of longitude
    lon_range = [-74.3, -73.6]
    if checkNull(lat) == 'VALID':
        if checkNumber(lon) and lon > lon_range[0] and lon < lon_range[1]:
            return 'VALID'
        else:
            return 'INVALID'
    else:
        return 'NULL'

