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
        if (date.year <=2015)&(date.year>=2005):
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