"""
Parses access_log file and inserts each log as a document to mongoDB.
It splits the file to 4 parts parsing each part then sending it to mongoDB as a group of documents.
an example of a document representing a log
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
import shlex
import time
import math

Months = {
    'Jan':  '01',
    'Feb':  '02',
    'Mar':  '03',
    'Apr':  '04',
    'May':  '05',
    'Jun':  '06',
    'Jul':  '07',
    'Aug':  '08',
    'Sep':  '09',
    'Oct':  '10',
    'Nov':  '11',
    'Dec':  '12'
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

DELIMITAR = ','
NULL = "\\N"

def parseLog(log):
    arr = shlex.split(log)
    record = ""
    record += arr[HOST] + DELIMITAR
    record += ( NULL if (arr[LOGNAME] == '-' or arr[LOGNAME] == "") else arr[LOGNAME]) + DELIMITAR
    record += (NULL if (arr[USER] == '-' or arr[USER] == "") else arr[USER]) + DELIMITAR

    date, time = arr[TIME][1:].replace(':', ' ',1).split(' ')
    date = date.split('/')
    date = date[2] + '-' + Months[date[1]] + '-' + date[0]
    ##record += date + " " + time + DELIMITAR
    
    req = (NULL if (arr[REQUEST] == '-' or arr[REQUEST] == "") else arr[REQUEST])
    record += req + DELIMITAR
    if(req != NULL):
        path = req.split()
        record += (path[1].lower() if len(path) == 3 else req.lower()) + DELIMITAR
    else:
        record += NULL + DELIMITAR
    record += (NULL if (arr[STATUS] == '-' or arr[STATUS] == "") else arr[STATUS]) + DELIMITAR
    record += (NULL if (arr[RESPONSE_SIZE] == '-' or arr[RESPONSE_SIZE] == "") else arr[RESPONSE_SIZE]) + DELIMITAR
    record += (NULL if (arr[REFERRER] == '-' or arr[REFERRER] == "") else arr[REFERRER])+ DELIMITAR
    record += (NULL if (arr[USER_AGENT] == '-' or arr[USER_AGENT] == "") else arr[USER_AGENT]) + '\n'
    return record
    
def main():
    logFile = open("access_log", "r")
    start = time.time() 
    lines = logFile.readlines()
    logFile.close()

    textFile = open("logs-hive.txt", "w")
    print("dividing records to ", NO_OF_BATCHES, "batchs")
    batchSize = len(lines)//NO_OF_BATCHES ##send data in 4 batches
    for i in range(0,math.ceil(len(lines)/batchSize)):
        records = []
        for line in range(i*batchSize, (i+1)*batchSize): 
            records.append(parseLog(lines[line]))
        textFile.writelines(records)
        print("writing batch no.", i+1)

    """
    records = []
    for line in range(0,10):
        records.append(parseLog(lines[line]))
    textFile.writelines(records)
    """
    textFile.close()
    end = time.time()
    print(end - start)
    
if __name__ == '__main__':
    main()
