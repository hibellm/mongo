
from pymongo import MongoClient  # Database connector
from bson.objectid import ObjectId  # For ObjectId to work
from bson.son import SON
import bson
import datetime as dt
import json

client = MongoClient('localhost', 27017)    # Configure the connection to the database

db = client.demo  # Select the database

dsilog = db.activityLog
dsiraw = db.rawrequest
dsireq = db.requests



def getdocs(col=None, query={}):
    if col:
        m = list(col.find(query, {"_id": 0}))
        print(f"{col.count_documents(query)} documents were retrieved from: {col.full_name} using query: {query}")
        return m
    else:
        print(f"No collection given - Stopping processing!")


def updatedocs(col=None, query={}, update={}):
    if col:
        print(f"{col.count_documents(query)} documents were found from: {col.full_name} using query: {query}")
        col.update_many(query, update)
        print(f"{col.count_documents(query)} documents were updated: using query: {update}")
        m = list(col.find(query))
        return m
    else:
        print(f"No collection given - Stopping processing!")


def enterlog(col=dsilog, ref=None, who=None, what=None, why=None, when=f"{dt.datetime.now()}"):
    # CREATE LOG DICTIONARY
    dsilogentry = {}
    if None not in (ref, who, what, why):
        dsilogentry['logref'] = ref
        dsilogentry['logwho'] = who
        dsilogentry['logaction'] = what
        dsilogentry['logreason'] = why
        dsilogentry['logDate'] = when
        try:
            a = col.insert_one(dsilogentry)
            print(f"Log entry was inserted: ObjectId: {a}")
        except Exception as e:
            print(f"Insert did not work : {e}")
    else:
        print("Need to enter all values for - Ref/who/what/why - Stopping processing!")




# IMPORT DATA INTO THE COLLECTIONS
with open('demo_raw.json', 'r') as myfile:
    data = myfile.read()

raw_insert = json.loads(data)
dsiraw.insert_many(raw_insert)

# REFERENCE LISTS
requesttypes = ['Data Sharing Request', 'Advice']
status = ['Active', 'In progress', 'On hold', 'Closed', 'Referred', 'Other', 'Unknown', 'Completed']
processed = [True, False, 'Error', '']

# PROCESS THE RAW INPUT INTO REQUEST

def proccessraw():
    '''
    Process the RAWREQUESTS.
    1.Find all new rawrequests that have a particular request Type and are not processed
    2.Make dictionary of new request
    3.Populate the requests collection, change status of rawrequest to Processed, enter log

    :return:
    '''
    # 1 - GET ADVICE/OTHER AND JUST MAKE A CLEAN ENTRY
    advice = getdocs(dsiraw, {'$and': [{"request.type": f"{requesttypes[1]}"}, {'processed': False}]})

    for a in advice:
        print(a)

        # PROCESS THE RAW REQUEST TO A PROCESSED REQUEST
        dsireqentry = {}
        dsireqentry['request'] = a['request']
        dsireqentry['requestor'] = a['requestor']
        dsireqentry['status'] = a['status'][0]  # OPEN
        dsireqentry['requestor'] = a['requestor']
        dsireqentry.pop('_id', None)
        dsireqentry['check'] = {"requestor": True, "study": True, "sharable": False, "contract": True}
        dsireqentry['allgood'] = all(value is True for value in dsireqentry['check'].values())

        # UPDATE THE RAW TO PROCESSED
        updatedocs(dsiraw, {"request.reference": f"{a['request']['reference']}"}, {'$set': {'processed': True}})

        enterlog(dsilog, status[0], a['request']['reference'], 'hibellm', "Request: Raw request to processed request", "Clean-up and population of tables")

        # SEND CONFIRMATION EMAIL FOR A REQUEST
        # sendmail("request",a['requestor']['email'], a['request']['reference'])

        enterlog(dsilog, status[-1], a['request']['reference'], 'hibellm', "Request: Email sent", f"Email sent to {a['requestor']['email']}")


    # 2 - GET ADVICE/OTHER AND JUST MAKE A CLEAN ENTRY
    dsr = getdocs(dsiraw, {'$and': [{"request.type": f"{requesttypes[0]}"}, {'processed': False}]})

    for d in dsr:
        # UPDATE THE RAW TO PROCESSED
        updatedocs(dsiraw, {"request.reference": f"{d['request']['reference']}"}, {'$set': {'processed': True}})
        # LETS PRINT OUT THE STUDIES REQUESTED
        dsireqentry = {}
        for i in d['requestdetail']['other']:
            ns = len(d['requestdetail']['other'])
            dsireqentry['request'] = d['request']
            dsireqentry['requestor'] = d['requestor']
            dsireqentry['requestdetail'] = {}
            dsireqentry['requestdetail']['summary'] = d['requestdetail']['summary']
            dsireqentry['requestdetail']['description'] = d['requestdetail']['description']
            dsireqentry['rochestudynumber'] = i['study']  # WOULD CHECK AGAINST THE VALID LIST
            dsireqentry['studydetails'] = i

            # CHECK IF STUDY IS SHARABLE
            # CHECK IF STUDY HAS CONTRACT
            # CHECK IF STUDY HAS RIGHT PATIENTS
            # CLEAN UP AND CHECK ENTRY
            dsireqentry.pop('_id', None)
            dsireqentry['check'] = {"requestor": True, "study": True, "sharable": False, "contract": True}
            dsireqentry['allgood'] = all(value is True for value in dsireqentry['check'].values())

            print(dsireqentry)
            dsireq.insert_one(dsireqentry)
            # dsireq.update_one({'_id': url}, {"$set": dsireqentry}, upsert=True)

        updatedocs(dsiraw, {"request.reference": f"{a['request']['reference']}"}, {'$set': {'processed': True}})
        enterlog(dsilog, status[0], d['request']['reference'], 'hibellm', f"Request: Raw request to processed request (Studies : {ns})",
                 "Clean-up and population of tables")



proccessraw()

processreq():
    which requests can be processed?
    make jira ticket?
    send emmail?
    perform taks?
    update status entry (to next level)
    enter task into log

checkallgood
    check all notgood entries
    has the status changed?
