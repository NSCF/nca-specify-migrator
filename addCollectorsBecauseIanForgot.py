# On the first import I realized that I had neglected to add the collectors. This is meant to be run with the same dataset to add the collectors in.
# It assumes that all the agents and collecting events are added, and simply joins them in the collectors table
# This ommission is fixed in the importdata script so it should not be necessary to run this script ever. 

import csv, sys, time
from os import path
from db.db import get_db
from db.utils.field_has_value import field_has_value
from db.utils.dict_is_empty import dict_is_empty
from Counter import Counter
from functions import find_or_add_record, get_record_data
from mappings import collectonObjectMapping
from db_credentials import credentials # A dictionary with fields for user, password, host, database, and collection name. Excluded for git for security reasons, so create your own. 

csvFile = r'NCA-data-export-20220901-OpenRefine-ie-edits20240313-coll12345Added-OpenRefine_20240624.csv'
csvDir = r'D:\NSCF Data WG\Specify migration\ARC PHP\NCA\SPECIFY\DATA'
maxCollectors = 5 # the maximum number of collectors for any record in the dataset

# for testing only - the index of the records to start processing at so we can fix errors. Set to None to process all records.
start_at = None
break_at = 10000 # stop after this number to check in the db, also only for testing. Set to None otherwise

### SCRIPT ###

agents = {}

start = time.perf_counter()

# the db connection parameters
db = get_db(*credentials.values())

collecting_events_processed = set()

counter = Counter(100)
exception = None
exception_table = None

collecting_events_not_found = []

print('processing records...')
print()
sys.stdout.write('\033[?25l') # to clear the console cursor for printing progress
sys.stdout.flush()

with open(path.join(csvDir, csvFile), 'r', encoding="utf8", errors='ignore') as f:
  reader = csv.DictReader(f)
  for record in reader:

    counter.increment()
    if break_at and counter.count >= break_at:
      break

    if start_at and counter.count < start_at:
      continue

    collection_object_data = get_record_data(record, collectonObjectMapping)

    #we don't capture anything for empty collection objects
    if dict_is_empty(collection_object_data):
      continue

    if not field_has_value('catalognumber', collection_object_data):
      continue

    try:
      collection_object_records = db.collectionobjects.find({ "catalognumber": collection_object_data["catalognumber"]} )
    except Exception as ex:
      exception = ex
      exception_table = 'collectionobject'
      break

    if len(collection_object_records) == 0:
      exception_table = 'collectionobject'
      exception = 'collection object not found'
      break

    if len(collection_object_records) > 1:
      exception_table = 'collectionobject'
      exception = 'multiple collection objects for catalog number'
      break

    collectionobject = collection_object_records[0]
    if not collectionobject["collectingeventid"]:
      collecting_events_not_found.append(collectionobject)
      continue
    elif collectionobject["collectingeventid"] in collecting_events_processed:
      continue

    # add the collectors
    collectorCount = 1 # because we start with collector 1
    collector_error = False
    while collectorCount <= maxCollectors:

      collecting_agent = {
        "title": record['coll' + str(collectorCount) + "title"],
        "firstname":  record['coll' + str(collectorCount) + "firstName"],
        "lastname": record['coll' + str(collectorCount) + "lastName"],
        "initials": record['coll' + str(collectorCount) + "initials"]
      }

      empties = []
      for key in collecting_agent.keys():
        if not collecting_agent[key]:
          empties.append(key)

      for key in empties:
        del collecting_agent[key]

      if dict_is_empty(collecting_agent):
        break

      try:
        agentid = find_or_add_record(db.agents, collecting_agent, agents, 'agentid')
      except Exception as ex:
        exception_table = 'collector'
        exception = str(ex)
        collector_error = False
        break

      if collector_error:
        break
      
      collector = {
        "agentid": agentid,
        "collectingeventid": collectionobject["collectingeventid"],
        "ordernumber": collectorCount - 1,
        "isprimary": collectorCount == 1
      }

      try:
        db.collectors.insert(collector)
      except Exception as ex:
        exception_table = 'collector'
        exception = str(ex)
        collector_error = True
        break

      collectorCount += 1

    if collector_error:
      break

    collecting_events_processed.add(collectionobject["collectingeventid"])
  
sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

end = time.perf_counter()
elapsed = end - start
hours, remainder = divmod(elapsed, 3600)
minutes, seconds = divmod(remainder, 60)

# for testing
db.rollback()

# if testing set a value exception in the debug console here to invoke the rollback 
if exception:
  print('error with', exception_table, f'row number {counter.count}')
  print('error message:', str(exception))
  print('rolling back database...')
  db.rollback()
  db.close()
  print('please fix the error and try again')
else:
  print(counter.count, 'records processed in', f"{int(hours):02}h{int(minutes):02}m{round(seconds):02}s")
  print('committing changes to the database...')
  db.commit()
  db.close()

  if len(collecting_events_not_found):
    print("Collecting events do not exist for the following records:")
    for collectionobject in collecting_events_not_found:
      print(collectionobject["catalognumber"])
    

  print('all done!')


      

