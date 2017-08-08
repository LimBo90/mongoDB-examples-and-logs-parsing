## Added some queries that can be done on the log collection

from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta

def findAllLogsForPage(path):
    """get all the logs for a particular page"""
    client = MongoClient("192.168.163.128" ,27017)
    logCol = client.testlocaldb.log
    return logCol.find({'path': path.lower()})

def findAllEventsForDate(date :datetime):
    """get all logs for a specific day"""
    client = MongoClient("192.168.163.128" ,27017)
    logCol = client.testlocaldb.log
    return logCol.find({'time': { '$gte': date,'$lt': date + timedelta(days=1)}})
    
def findAllEventsForHostAndDate(host, date):
    """get all logs for a specific host and day"""
    client = MongoClient("192.168.163.128" ,27017)
    logCol = client.testlocaldb.log
    return logCol.find({'host': host,'time': { '$gte': date,'$lt': date + timedelta(days=1)}})

def getHitsPerPageInDay(date):
    """for aspecific day get a list of pages and the number times they were accessed"""
    client = MongoClient("192.168.163.128" ,27017)
    logCol = client.testlocaldb.log
    results = logCol.aggregate([
         {  '$match': {
               'time': {
                   '$gte': date,
                   '$lt':  date + timedelta(days=1) } } },
         {  '$project': {
                 'path': 1 } },
         { '$group': {
                 '_id': {'page':'$path' },
                 'hits': { '$sum': 1 } } }
         ])
    return prettify(results)
    
def prettify(cmdCursor):
    """Convert the results of getHitsPerPageInDay to alist of dictionaries with each dicitionary has page and hits keys"""
    results = []
    for entry in cmdCursor:
        page = entry.get('_id').get('page')
        hits = entry.get('hits')
        results.append({'page' : page, 'hits': hits})
    return results

def printRecords(results):
    for result in results:
        print(result)

