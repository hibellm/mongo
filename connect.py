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


# FIND SOMETHING
x = vendors.find({"name": "Marcus Hibell"})
# PRINT OUT THE RESULTS
for doc in x:
    print(doc)

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

query = {'dataobject.type': "table"}
print(f"The number of observations retrieved : {dsido.count_documents(query)}")

x = dsido.find()
# PRINT OUT THE UPDATED RESULTS
for doc in dsido.find():
    print(doc)

update = {'$currentDate': {"lastmodifieddate.date": {'$type': "timestamp"}}}
result = dsido.update_many(query, update)
for doc in result:
    print(doc)



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



chardet.detect(s)
print(s)
