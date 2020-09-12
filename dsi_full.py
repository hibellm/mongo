
from pymongo import MongoClient  # Database connector
from bson.objectid import ObjectId  # For ObjectId to work
from bson.son import SON
import bson
import datetime as dt
import json
import pickle

client = MongoClient('localhost', 27017)    # Configure the connection to the database

db = client.demo  # Select the database

dsilog = db.activityLog
dsiraw = db.rawrequest
dsireq = db.requests

'''
So the Process should be

1. take raw and move to request
2. check each criteria
 - if passed set to True and update the modified date
3. if allgood is False...and modified recently, check if it is now True
 - if true run the process 
'''

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
# GET THIS LIST FROM THE DICTIONARY
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

    if advice:
        for a in advice:
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

            enterlog(dsilog, status[-1], a['request']['reference'], 'hibellm', f"Confirmation Email (Request Acknowledgement) sent to {a['requestor']['email']}")
    else:
        # NO ADVICE TO PROCESS
        pass

    # 2 - GET ADVICE/OTHER AND JUST MAKE A CLEAN ENTRY
    dsr = getdocs(dsiraw, {'$and': [{"request.type": f"{requesttypes[0]}"}, {'processed': False}]})

    if dsr:
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
                dsireqentry['status'] = d['status']
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
    else:
        # NO DSR TO PROCESS
        pass

proccessraw()


# PROCESS THE CLEAN REQUESTS - WHICH ARE ALLGOOD = True
def processreq():

    requests = getdocs(dsireq, {'$and': [{"": f"{requesttypes[0]}"}, {'allgood': True}]})

    for r in requests:
        jira = makejira(r['rochestudynumber'])
        addlabel('Oncology')
        updatedocs(dsiraw, {"request.reference": f"{r['request']['reference']}"}, {'$set': {'processed': True}})
        updatedocs(dsireq, {"request.reference": f"{r['request']['reference']}"}, {'$addtoset': {['in progress', dt.datetime.now(), 'dsi_code']}})

        enterlog(dsilog, status[0], r['request']['reference'], 'hibellm',
                 f"Request: Jira Ticket created to process request (JIRA : {jira.key})",
                 "Clean-up and population of tables")

    which requests can be processed?
    make jira ticket?
    send email?
    perform taks?
    update status entry (to next level)
    enter task into log



# CHECK WHAT IS STILL NOT OKAY
def chkallgood():
    '''
    check which documents are allgood=False. Check when last updated.
    If updated within the last 20 mins - run the check on that.
    :return:
    '''
    docs = getdocs(dsireq, {"$and": [{"allgood": False}, {"request.date": {"$gte": f"{dt.datetime.now()-dt.timedelta(days=100)}" }}]})

    # RUN THE CHECK
    for doc in docs:
        if doc['requestor'] == False:
            chkrequestor()
        if doc['requestor'] == False:
            chkrawpath()
        if doc['requestor'] == False:
            chkanapath()
        if doc['requestor'] == False:
            chkcontract()
        if doc['requestor'] == False:
            chkready()
        if doc['requestor'] == False:
            chk()


    # LAST THING IS TO RECHECK IF ALL IS GOOD AND PROCESS
    docs = getdocs(dsireq, {"$and": [{"allgood": True}, {"request.date": {"$gte": f"{dt.datetime.now()-dt.timedelta(days=100)}" }}]})
    theprocess()


# CHECK FOLDER IS ACCESSIBLE/VALID
def chkrawpath():
    '''
    update Mongo.request.anapath to tru if path is valid and accessible and something in there
    :return:
    '''
    if valid:
        updatedocs(dsireq, {"request.reference": f"{a['request']['reference']}"}, {'$set': {'processed': True}})
        updatedocs(dsireq, {"request.reference": f"{a['request']['reference']}"}, {'$set': {'modifieddate': dt.datetime.now()}})
        enterlog(dsilog, status[0], r['request']['reference'], 'hibellm',
                 f"Checking raw path is valid and accessible",
                 f"Path {path} is valid and accessible for anonymization")


def chkallgood():
    '''
    check all components to see if everything is ok.
    If yes then kick off the process
    :return:
    '''
    if all([True,True,False]):
        print("false")
    else:
        updatedocs(dsireq, {"request.reference": f"{a['request']['reference']}"}, {'$set': {'allgood': True}})
        updatedocs(dsireq, {"request.reference": f"{a['request']['reference']}"}, {'$set': {'modifieddate': dt.datetime.now()}})
        # update jira from on hold to in-progress
        enterlog(dsilog, status[0], r['request']['reference'], 'hibellm',
                 f"Checking all criteriaChecking raw path is valid and accessible",
                 f"All criteria have passed -  Setting to allgood=True")

def theprocess():
    '''
    Run the anonymization process
    :return:
    '''
    # Run the process to anonymize - USE A QUEUE SYSTEM TO LINE UP



import operator
from operator import itemgetter
# UTILITY FUNCTIONS
def createreport():
    logs = getdocs(dsilog, {})
    # EXTRACT THE REFERENCE AND THE DATE FOR SORTING
    xlogs = []
    for log in logs:
        xlogs.append([log['logwho'] , log['logDate'], log])

    sorted(xlogs, key=itemgetter(0,1))

    # HEADINGS
    print(f"Log Report : {dt.datetime.now().strftime('%d-%b-%Y %H:%M:%S')}\n{'='*33}\n")
    print(f"Reference\n{'-'*36}")
    for xlog in xlogs:
        print(f"{xlog[2]['logwho']}")
        print(xlog)
        # PRINT OUT TO A HTML FILE AND THEN TO PDF IF YOU WANT.


createreport()

# COULD TRY MONGOEXPORT BUT OVERKILL
def archivelog():
    # FIND ALL ENTRIES BETWEEN DATES - EXCEPT THE ARCHIVE LOG ENTRY
    logs = getdocs(dsilog, {'$and' : [{"request.date": {"$gte": f"{dt.datetime.now()-dt.timedelta(days=100)}" }},
                                      {'$ne': 'Not applicable'}]})

    # GET THE LIST OF _IDS TO REMOVE ONCE SAVED
    # EXPORT THE LOGS TO A JSON FILE
    with open(f"./logarchive_{dt.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.json", 'w') as fp:
        json.dump(logs, fp)

    # ADD ENTRY TO LOG
    enterlog(dsilog, status[0], 'Not applicable', 'hibellm',
             f"ARCHIVE: Action log entries [scheduled process]",
             f"The log entries for [x to y] have been archived to a JSON file: {filepath}")

    # DELETE THE LOG ENTRIES
    for i in logs:
        delete where _id=ObjectId(i)




def findarchive(date):
    logs = getdocs(dsilog, {"logaction": "ARCHIVE: Action log entries [scheduled process]" })

    if isinstance(date, date):
        pass
    else:
        print(f"Pass in a date, eg dt.datetime()")



    for log in logs:
        dates = str(log['logreason'][log['logreason'].find('[') + 1:log['logreason'].find(']')]).split('to')

        if dt.datetime.strptime(dates[0].strip(), '%Y-%m-%d %H:%M:%S') <= date <= dt.datetime.strptime(dates[-1].strip(), '%Y-%m-%d %H:%M:%S'):
            print("in between")
        else:
            print("No!")

        startdt =
        stopdt = log['why']
        logfile = log['why'][log['why'].find(":")+1:]

        log = openarchive (logfile)
        print(log)



def openarchive(logfile=None):
    if logfile:
        try:
            with open(logfile, 'r') as myfile:
                data = myfile.read()
            return json.loads(data)
        except Exception as e:
            print(f"Could not read the JSON file '{logfile}' - {e}")
    else:
        print(f"No logfile defined - Stopping Processing!")

openarchive("./logarchive_2020_09_11_22_28_51.json")


def checkstudy():
    if study is a rochestudynumber
    check already anonymized (search through deliverables)
    check rawpath
    check anapath

def initiateanon():
    if allgood, can initiate anon

