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
    if len(string.strip())==0:
        return 'NULL'
    # Step 2: Check for 'nan'
    elif string=='nan':
        return 'NULL'
    else:
        return 'VALID'

# If date is in wrong format it would not be sucessfully converted to datetime object.
def checkDate(line,date_format=date_format):
    if checkNull(line)=='VALID':
        try:
            date = datetime.datetime.strptime(line,date_format)
            if (date.year <=2015)&(date.year>=2005):
                return 'VALID'
            else:
                return 'INVALID'
        except:
            return 'INVALID'
    else:
        return 'NULL'
def checkTime(line,time_format=time_format):
    if checkNull(line)=='VALID':
        try:
            datetime.datetime.strptime(line,time_format)
            return 'VALID'
        except:
            return 'INVALID'
    else:
        return 'NULL'
########################################
    
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
# Implementation
# check_CMPLNT_TO check both CMPLNT_TO_DT and CMPLNT_TO_TM at the same time
# It return the validity of both field
def check_FR_TO(from_date,from_time,to_date,to_time,date_format,time_format):
    ###########
    #Null check
    ###########
    individual_checks = [checkDate(from_date),checkTime(from_time),checkDate(to_date),checkTime(to_time)]
    if 'NULL' in individual_checks:
        return individual_checks
#         if 'NULL' in individual_checks[:2]:
#             individual_checks = ['NULL','NULL','NULL','NULL']
#             return individual_checks
#         if 'NULL' in individual_checks[2:4]:
#             return individual_checks
    #Check for invalidity
    #If any field format is invalid return individual check
    elif 'INVALID' in individual_checks:
        return individual_checks
    else:
        #ALL VALID for individual check
        ###Start combining
        #Define datetime format
        datetime_format = date_format+' '+time_format
        from_datetime = datetime.datetime.strptime(from_date+' '+from_time,datetime_format)
        to_datetime = datetime.datetime.strptime(from_date+' '+from_time,datetime_format)
        if from_datetime>to_datetime:
            return ['VALID','VALID','INVALID','INVALID'] 
        else:
            return individual_checks    
    

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
        if checkNumber(lat) and float(lat) > lat_range[0] and float(lat) < lat_range[1]:
            return 'VALID'
        else:
            return 'INVALID'
    else:
        return 'NULL'

def checkLongitude(lon):
    # check the validation of longitude
    lon_range = [-74.3, -73.6]
    if checkNull(lon) == 'VALID':
        if checkNumber(lon) and float(lon) > lon_range[0] and float(lon) < lon_range[1]:
            return 'VALID'
        else:
            return 'INVALID'
    else:
        return 'NULL'

