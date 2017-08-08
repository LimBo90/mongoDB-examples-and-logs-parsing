"""
parses the access_log file and insert each record as a document to mongoDB.
It splits the file to 4 parts for each part spawning 2 process for parsing
then sending it to the mongoDB.
an example of a document representing a log.
{
        "_id" : ObjectId("5984ee5d4b5d282a4c2e1e54"),
        "host" : "108.163.152.5",
        "logname" : null,
        "user" : null,
        "time" : ISODate("2016-12-06T08:05:23Z"),
        "request" : "GET /cgi-sys/defaultwebpage.cgi HTTP/1.1",
        "path" : "/cgi-sys/defaultwebpage.cgi",
        "status" : 200,
        "response_size" : 6734,
        "referrer" : null,
        "user_agent" : "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:49.0) Gecko/20100101 Firefox/49.0"
}
"""

from pymongo import MongoClient
from multiprocessing import Pool
from datetime import datetime
import re

import shlex
import time
import math

Months = {
    'Jan': 1,
    'Feb': 2,
    'Mar': 3,
    'Apr':4,
    'May':5,
    'Jun':6,
    'Jul':7,
    'Aug':8,
    'Sep':9,
    'Oct':10,
    'Nov':11,
    'Dec':12
    }

HOST = 0
LOGNAME = 1
USER = 2
TIME = 3
REQUEST = 5
STATUS = 6
RESPONSE_SIZE =7
REFERRER = 8
USER_AGENT = 9
NO_OF_BATCHES = 4

def parseLog(log):
    arr = shlex.split(log)
    document = {}
    document["host"] = arr[HOST]
    document["logname"] = None if (arr[LOGNAME] == '-' or arr[LOGNAME] == "") else arr[LOGNAME]
    document["user"] = None if (arr[USER] == '-' or arr[USER] == "") else arr[USER]
    timeArr = re.split("/|:", arr[TIME][1:])
    day = int(timeArr[0])
    month = Months.get(timeArr[1])
    year = int(timeArr[2])
    hrs = int(timeArr[3])
    mins = int(timeArr[4])
    secs = int(timeArr[5])
    
    document["time"] = datetime(year,month,day,hrs,mins,secs)
    document["request"] = None if (arr[REQUEST] == '-' or arr[REQUEST] == "") else arr[REQUEST]
    if(document["request"] != None):
        path = document["request"].split()
        document["path"] = path[1].lower() if len(path) == 3 else document["request"].lower()
    else:
        path = None
    document["status"] = None if (arr[STATUS] == '-' or arr[STATUS] == "") else int(arr[STATUS])
    document["response_size"] = None if (arr[RESPONSE_SIZE] == '-' or arr[RESPONSE_SIZE] == "") else int(arr[RESPONSE_SIZE])
    document["referrer"] = None if (arr[REFERRER] == '-' or arr[REFERRER] == "") else arr[REFERRER]
    document["user_agent"] = None if (arr[USER_AGENT] == '-' or arr[USER_AGENT] == "") else arr[USER_AGENT]
    return document




def main():
    logFile = open("access_log", "r")
    start = time.time()
    lines = logFile.readlines()
    logFile.close()
    print (len(lines))
    
    client = MongoClient("192.168.163.128" ,27017)
    logCol = client.testlocaldb.log

    ##parse and send
    batchSize = len(lines)//NO_OF_BATCHES ##send data in 4 batches 
    for i in range(0,math.ceil(len(lines)/batchSize)):
        documents = Pool(2).map(parseLog, lines[i*batchSize:(i+1)*batchSize])   ##initalize threads to parse logs from one of the 4 batchs to documents 
        logCol.insert_many(documents)   ##send all documents in the batch
#    logCol.insert(parseLog(lines[382]))
    logCol.ensure_index('time')
    logCol.ensure_index([('time', 1), ('host', 1)])
    client.close()
    end = time.time()
    print(end - start)

    
if __name__ == '__main__':
    main()
