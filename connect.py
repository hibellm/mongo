#!/usr/bin/python3
# -*- encoding: utf-8 -*-

"""
CREATED    : 05DEC2019
AUTHOR     : HIBELLM
DESCRIPTION: Connect to MongoDB instance
"""

from pymongo import MongoClient  # Database connector
from bson.objectid import ObjectId  # For ObjectId to work
from bson.son import SON
import bson
import datetime as dt
import json

client = MongoClient('localhost', 27017)    # Configure the connection to the database
db = client.mdh       # Select the database
vendors = db.vendors  # Select the collection

dsidb = client.dsi    # Select the database
dsido = dsidb.DataObject
dsidv = dsidb.DeliverObject
dsidict = dsidb.dictionary
dsistudy = dsidb.StudyMetadata
dsitrain = dsidb.train




def getdocs(col=None, query={}):
    if col:
        m = list(col.find(query))
        print(f"{col.count_documents(query)} documents were retrieved from: {col.full_name} using query: {query}")
        return m
    else:
        print(f"No collection given - Stopping processing!")


docs = getdocs(dsitrain, {})
docs = getdocs(dsidv, {})
docs = getdocs(vendors, {"name": "Marcus Hibell"})
docs = getdocs(vendors, {"name": "Marcus Hibell", "last_modified": {"$exists" : "true"}})
docs = getdocs(vendors, {"name": "Marcus Hibell", "last_modified": {"$gt" : dt.datetime(2020,7,27)}} )


def updatedocs(col=None, query={}, update={}):
    if col:
        print(f"{col.count_documents(query)} documents were found from: {col.full_name} using query: {query}")
        col.update_many(query, update)
        print(f"{col.count_documents(query)} documents were updated: using query: {update}")
        m = list(col.find(query))
        return m
    else:
        print(f"No collection given - Stopping processing!")

udocs = updatedocs(vendors, {"name": "Marcus Hibell"}, {'$set': {'code': 'mjhxxx'}})
udocs = updatedocs(vendors, {"name": "Marcus Hibell", "last_modified": {"$exists" : "true"}}, {'$set': {"last_modified": dt.datetime.utcnow()}})
udocs = updatedocs(vendors,
                   {"name": "Marcus Hibell", "last_modified": {"$gt" : dt.datetime(2020,7,27)}},
                   {'$set': {"last_modified": dt.datetime.utcnow()}})


def insertone(col=None, insert={}):
    if col:
        try:
            a = col.insert_one(insert)
            print(f"Document was inserted: ObjectId: {a}")
        except Exception as e:
            print(f"Insert did not work : {e}")
    else:
        print(f"No collection given - Stopping processing!")

# INSERT A DATE
idocs = insertone(vendors, {"test_modified": dt.datetime.utcnow()})


def insertmany(col=None, insert=[]):
    if col:
        try:
            col.insert_many(insert)
            print(f"{len(insert)} Document was inserted")
        except Exception as e:
            print(f"Insert did not work : {e}")
    else:
        print(f"No collection given - Stopping processing!")

# INSERT MANY (A LIST OF DICTIONARIES)
insertmany(vendors, [{"test_modified": dt.datetime.utcnow()}])


#ADD TO AN ARRAY (LIST) - IF EXISTING WILL NOT ADD
col = dsidict
col.update_one({"_id" : ObjectId("5e0913e9648e3412bcf19178")}, { "$addToSet": { "varlist": 'xxx'}})
col.update_one({"_id" : ObjectId("5e0913e9648e3412bcf19178")}, { "$addToSet": { "varlist": ['xxx','yyy','zzz']}})
col.update_one({"_id" : ObjectId("5e0913e9648e3412bcf19178")}, { "$addToSet": { "varlist": {"$each": ['xxx','yyy','zzz']}}})

#POP item from array - the numbering is reverse of python (-1 = first
col.update_one( {"_id" : ObjectId("5e0913e9648e3412bcf19178")}, { "$pop": { "varlist": -1 }})   #POP FIRST
col.update_one( {"_id" : ObjectId("5e0913e9648e3412bcf19178")}, { "$pop": { "varlist": -2 }})   #POP FIRST TWO
col.update_one( {"_id" : ObjectId("5e0913e9648e3412bcf19178")}, { "$pop": { "varlist": 1 }})   #POP LAST
col.update_one( {"_id" : ObjectId("5e0913e9648e3412bcf19178")}, { "$pull": { "varlist": 1 }})   #PULL LAST

#PULL IS LIIKE POPING -  BUT WORKS ON THE VALUE NOT THE POSITION
col.update_one( { }, { "$pull": { "fruits": { "$in": [ 'apples', 'oranges' ] }, "vegetables": 'carrots' }}, { "multi": "true" })
col.update( { "_id": 1 }, { "$pull": { "votes": { "$gte": 6 }}})
col.update({ "_id": 1 }, { "$push": { "scores": {"$each": [ 50, 60, 70 ], "$position": 2 }}})

#AGGREGATION
col.aggregate( [ {"$addFields": { "totalHomework": { "$sum": '$homework' }, "totalQuiz": { "$sum": '$quiz' }}},
                 {"$addFields": { "totalScore": { "$add": [ '$totalHomework', '$totalQuiz', '$extraCredit' ] }}}
] )



# COMMAND LINE EXPORT
mongoexport -d=dbname -c=collection -q='{ "metrictimestamp": { "$gte": { "$date": "2018-02-01T00:00:00Z" }, "$lt": { "$date": "2018-02-02T00:00:00Z" } } }'



col=vendors
query = {"name": "Marcus Hibell"}
insert = {"test_modified": dt.datetime.utcnow()}
col.insert(insert)

col.insert_many()



insertdt = updatedocs(vendors, {"name": "Marcus Hibell"}, {"last_modified": dt.datetime.utcnow()})


"dt" :
{
    "$gte" : ISODate("2014-07-02T00:00:00Z"),
    "$lt" : ISODate("2014-07-03T00:00:00Z")
}
# COUNT SOMETHING
print(f"The number of observations retreived : {vendors.count_documents({'name': 'Marcus Hibell'})}")

# UPDATE SOMETHING
result = vendors.update_many({'code': 'mjh'}, {'$set': {'code': 'mjhxxx'}})
x = vendors.find({"name": "Marcus Hibell"})
# PRINT OUT THE UPDATED RESULTS
for doc in x:
    print(doc)

# INSERT A DATE
result = dsido.insert_one({"last_modified": dt.datetime.utcnow()})


# UPDATE SOMETHING
# https://docs.mongodb.com/manual/reference/operator/update-field/
# https://docs.mongodb.com/manual/reference/operator/query/

x = 'table'
query = {'dataobject.type': x}
print(f"The number of observations retrieved : {dsido.count_documents(query)}")

x = dsido.find()
# PRINT OUT THE UPDATED RESULTS
for doc in dsido.find():
    print(doc)

studyx = 'BP27272'
query = {'study_number': studyx}
print(f"The number of observations retrieved : {dsistudy.count_documents(query)}")


https://www.tutorialspoint.com/check-for-null-in-mongodb

update = {'$currentDate': {"lastmodifieddate.date": {'$type': "timestamp"}}}
result = dsido.update_many(query, update)
for doc in result:
    print(doc)



# WORKING WITH STUDY INFO
import json
from bson import BSON
from bson import json_util
db = client.dsi  # Select the database
dsismd = db.StudyMetadata  # Select the collection
query = {}
print(f"The number of observations retrieved : {dsismd.count_documents(query)}")

y = []
x = (dsismd.find(query))
for doc in x:
    y.append(doc)

study = 'ABC1234'
# LOOKING IN ANOTHER DOCUMENT
query = {'dataobject.location.database': f'ds_ctdc_{study.lower()}'}
print(f"The number of observations retrieved : {dsido.count_documents(query)}")
# OPEN A FILE

f = open("myfile.txt", "w")
x = (dsido.find(query))
for doc in x:
    print(doc['dataobject']['type'])
    f.write(json.dumps(doc, sort_keys=True, indent=4, default=json_util.default))
f.close()




# UPDATE THE FIELDS IN A LIST IN DO database
# get the objects checksums of interest
query = {'dataobject.location.database': 'ds_ctdc_abc123'}
filter = {'dataobject.checksum': 1}
result = dsido.find(query, filter)
print(f"The number of observations retrieved : {dsido.count_documents(query)}")
for doc in result:
    print(doc)

# update the delivery with the object checksums
query1 = {"_id": ObjectId("5ded169be441cef45086bfbf")}

for doc in dsido.find(query, filter):
    print(f"Adding this ObjectId to the contents {doc['_id']}")
    update = {'$addToSet': {"deliverobject.contents": {'$each': [doc['_id']]}}}
    print(update)
    dsidv.update_one(query1, update)



# LOAD FILE TO MONGO
with open('./assets/import.json') as f:
    file_data = json.load(f)

dsido.insert_one(file_data)



# TAKE A LIST OF FILES AND LOAD AS OBJECTS THEN UPDATE
mylist = [['eg', 'T'], ['EX', 'T'], ['MH', 'T'], ['LB', 'T'], ['ZA', 'T'], ['ae_RCR', 'V']]

# HOW MANY BEFROE WE LOAD
print(f"The number of observations before : {dsido.count_documents({})}")

for item in mylist:
    if item[1] == 'T':
        query = {"dataobject": {"type": "Table", "filename": f"{item[0].lower()}"}}
        print(query)
    else:
        query = {"dataobject": {"type": "View", "filename": f"{item[0].lower()}"}}
        print(query)
    dsido.insert(query)

# HOW MANY AFTER WE LOAD
print(f"The number of observations before : {dsido.count_documents({})}")


# HOW MANY ELEMENTS IN A FIELD
pipeline = [{"$unwind": "$varlist"},
            {"$group": {"_id": "$name", "count": {"$sum": 1}}},
            {"$sort": SON([("count", -1), ("_id", -1)])}
            ]

print(list(db.dictionary.aggregate(pipeline)))


# HOW MANY ELEMENTS IN A FIELD
pipeline = [{"$unwind": "$varlist"},
            {"$group": {"_id": "$name", "count": {"$sum": 1}}},
            {"$sort": SON([("count", -1), ("_id", -1)])}
            ]

pipeline = [{"$group": {"_id": "$name", "myCount": {"$sum": 1}}}]
print(list(db.dictionary.aggregate(pipeline)))

db.dictionary.aggregate( [ { "$collStats": { "latencyStats": { "histograms": True } } } ] )

# STATISTICS
list(db.dictionary.aggregate([{"$collStats": {"latencyStats": {"histograms": True}}}]))

list(db.dictionary.aggregate([{"$collStats": {"count": {}}}]))



# NOW TIME TO UPDATE

















# GET SOME FILE DETAILS
# GUESS ENCODING OF FILE
import chardet
import urllib3
import requests

r = requests.get('https://api.github.com/events')
r.text
r.encoding

file = open("connect.py", "r")
file.encoding
file = open("./assets/ansi.txt", "r")
file.encoding
file = open("./assets/utf-8.txt", "r")
file.encoding
file = open("./assets/ansi_unix.txt", "r")
file.encoding
file = open("./assets/utf-8_unix.txt", "rb")
chardet.detect(file)







import subprocess
subprocess.call("chardetect", "./connect.py")

import urllib3
http = urllib3.PoolManager()

url = 'http://www.thefamouspeople.com/singers.php'
response = http.request('GET', url)
import chardet
chardet.detect(response)
