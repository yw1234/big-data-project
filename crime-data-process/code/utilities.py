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

def checkDate(line,date_format=date_format):
    try:
        date = datetime.datetime.strptime(line,date_format)
        if (date.year <= 2015) & (date.year >= 2006):
            return 'VALID'
        else:
            return 'INVALID'
    except:
        return 'INVALID'
    
def checkTime(line,time_format=time_format):
    try:
        datetime.datetime.strptime(line,time_format)
        return 'VALID'
    except:
        return 'INVALID'
    
def checkRegex(line,regex):
    # Input
    # line: a string for check
    # regex: regular expression pattern
    match = re.match(regex,line)
    if match:
        return 'VALID'
    else:
        return 'INVALID'

# Implementation
# check_CMPLNT_TO check both CMPLNT_TO_DT and CMPLNT_TO_TM at the same time
# It return the validity of both field
def check_FR_TO(from_date,from_time,to_date,to_time,date_format,time_format):
    ###########
    #Null check
    ###########
    from_nullity = [checkNull(from_date),checkNull(from_time)]
    to_nullity = [checkNull(to_date),checkNull(to_time)]
    # If from datetime is NULL, return all NULL
    if 'NULL' in from_nullity:
        return ['NULL','NULL','NULL','NULL']
    elif 'NULL' in to_nullity:
        if 'INVALID' in [checkDate(from_date),checkTime(from_time)]:
            return ['INVALID','INVALID','NULL','NULL']
        else:
            return ['VALID','VALID','NULL','NULL']
    #Check for invalidity
    #If any field format is invalid return invalid for both
    elif ('INVALID' in [checkDate(x) for x in [from_date,to_date]])|('INVALID' in [checkTime(x) for x in [from_time,to_time]]):
        return ['INVALID','INVALID','INVALID','INVALID']    
    else:
        ###Start combining
        #Define datetime format
        datetime_format = date_format+' '+time_format
        from_datetime = datetime.datetime.strptime(from_date+' '+from_time,datetime_format)
        to_datetime = datetime.datetime.strptime(from_date+' '+from_time,datetime_format)
        if from_datetime <= to_datetime:
            return ['VALID','VALID','VALID','VALID']
        else:
            # Or ['INVALID','INVALID','INVALID','INVALID']? Open for discussion
            
            return ['VALID','VALID','INVALID','INVALID']      
    

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

