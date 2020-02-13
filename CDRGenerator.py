
# coding: utf-8
import numpy as np
import random
import csv
import uuid
import datetime
import os
import logging
import boto3

debug = 0


# Read in the area codes

def get_area_codes():
    
    print("Getting area codes")

    f = open("uk-area-code-list.csv", "r", encoding="utf-8-sig")

    csvfile = csv.reader(f)
    areacodes = []

    for x in csvfile:
        if (debug):
            print (x)
        areacodes.append(x[0])

    f.close()

    print("Number of area codes is", len(areacodes))

    return areacodes


# Read in the country codes

def get_country_codes():

    print("Getting country codes")

    f = open("country-codes.csv", "r", encoding="utf-8-sig")

    csvfile = csv.reader(f)
    countrycodes = []

    for x in csvfile:
        if (debug):
            print (x)
        countrycodes.append(x[0])

    f.close()
    
    print("Number of country codes is", len(countrycodes))

    return countrycodes


# Small function that return TRUE with a specified probability

def probability(p):
    
    # p is a percentage
    # Get a number between 1 and 200
    
    r = random.random() * 100
    
    if (r < p):
        rv = 1
    else:
        rv = 0
      
    return rv


# Construct a UK only MSISDN
# Calling country code is always 44
# Choose the first digit as 7 or something else - it should be a 7, 25% of the time.
# if 7 add the remaing 9 digits of the MSISDN 
# If not 7 then normall choose an area code () and occasionally make a 4 digit area code up
# then add a 6 digit number

def get_uk_msisdn(areacodes):
    
    cc = '44'
    ac = []

    if (probability(25)):
        ac = '7' + str(random.randint(100, 999))
    elif (probability(90)):
        ac =  areacodes[random.randint(0, len(areacodes)-1)]
    else:
        ac = str(random.choice(['3', '4', '5', '8', '9'])) + str(random.randint(100, 999))
  
    msisdn = cc + ac + str(random.randint(100000, 999999))
    
    return msisdn
    
# Construct any MSISDN, international allowed but mostly UK
# 95% of the time return a UK MSISDN
# 5% of the time get a valid countro code followed by 10 digits

def get_any_msisdn(areacodes, countrycodes):
    
    if (probability(95)):
        msisdn = get_uk_msisdn(areacodes)
    else:
        cc = countrycodes[random.randint(0, len(countrycodes)-1)]
        msisdn = cc + str(random.randint(1000000000, 9999999999))
  
    return msisdn
    
def get_cdr(areacodes, countrycodes):
    
    id = str(uuid.uuid4())

    start = datetime.datetime.now()
    end = start + datetime.timedelta(0,random.randint(1, 999))

    calling_msisdn = get_uk_msisdn(areacodes)

    called_msisdn = get_any_msisdn(areacodes, countrycodes)

    if (probability(80)):
        call_type = 'Voice'
    else:
        call_type = 'SMS'

    charge = str(   int(random.random() * 1000) / 100  )

    if (probability(80)):
        call_result = 'SUCCESS'
    else:
        call_result = 'FAILURE'
        
    cdr = str(id) + ', ' + calling_msisdn + ', ' + called_msisdn + ', '
    cdr = cdr + str(start) + ', ' + str(end) + ', ' + call_type + ', '
    cdr = cdr + charge + ', ' +  call_result
    
    return cdr


# set batches to the required number
batches = int(os.getenv('BATCHES', 2))

# set files_per_batch to the required number
files_per_batch = int(os.getenv('FILES_PER_BATCH', 3))

# set records_per_file to the required number
records_per_file = int(os.getenv('RECORDS_PER_FILE', 10))

prefix = os.getenv('PREFIX', datetime.datetime.utcnow().isoformat())


# f = open("cdr.csv", "w")

areacodes = get_area_codes ()
countrycodes = get_country_codes()

bucketName = os.getenv('BUCKET')
s3 = boto3.client('s3')

for b in range(batches):
    print ("Batch ", b)
    for f in range(files_per_batch):
        filename = prefix + "-B" + str(b) + "F" + str(f) + "cdr.csv"
        filepath = "/tmp/" + filename
        
        fh = open(filepath, "w")
        
        # Add header
        fh.write("id, calling, called, start, end, call_type, charge, call_result\n");

        for l in range(records_per_file):

            cdr = get_cdr(areacodes, countrycodes)
            fh.write(cdr + "\n")

        fh.close()
        
        print(l+1, "records written to file " + filepath + ".")

        print("copy file " + filename + " to bucket " + bucketName)
        with open(filepath, 'rb') as f:
            s3.upload_fileobj(f, bucketName, filename)

        print("delete local file " + filename)
        os.remove(filepath)


    

