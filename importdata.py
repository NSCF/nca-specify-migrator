### The import script
### REMEMBER TO MAKE A BACKUP OF YOUR DATABASE BEFORE YOU DO THIS!
### Make sure to run check_taxa_or_geography first so that this doesn't stop part way when missing taxa are found (for both taxa and geography).
### Also run check_coordinates first to fix any errors there so we don't have problems here.
### Note that for coordinates that don't convert (there will always be some) these will be added to verbatim coordinates.
### Ideally test this script in the debugger in VS Code on a local copy of the database, and continue (not stop) after catching exceptions so it can roll back the database.
### Then run it in a regular terminal on the live database when all the wrinkles are ironed out, it should be a little faster.

import csv, sys, time
from os import path
from db.db import get_db
from db.utils.field_has_value import field_has_value
from db.utils.dict_is_empty import dict_is_empty
from Counter import Counter
from functions import find_or_add_record, get_date_precision, fix_date, get_record_data
from mappings import collectonObjectMapping, collectionObjectAttributesMapping, \
   determinationMapping, localityMapping, collectingEventMapping, \
  collectingEventAttributesMapping, determinerMapping, collectingTripMapping, geographyMapping, taxonMapping

from db_credentials import credentials # A dictionary with fields for user, password, host, database, and collection name. Excluded for git for security reasons, so create your own. 

# clone the coords parser repo from https://github.com/NSCF/geo-coords-parser-python
# this may have to change depending where you put the repo
sys.path.append(r"C:\devprojects\geo-coords-parser-python")
from converter import convert

csvFile = r'NCA-data-export-20220901-OpenRefine-ie-edits20240313-coll12345Added-OpenRefine_20240624.csv'
csvDir = r'D:\NSCF Data WG\Specify migration\ARC PHP\NCA\SPECIFY\DATA'
maxCollectors = 5 # the maximum number of collectors for any record in the dataset
prepTypeID = 21 # wet specimen/s

# we need to specify the coordinate fields here because they should not be in the mappings
dataset_latitude_field = 'MigrationLatitude' # case sensitive
dataset_longitude_field = 'MigrationLongitude' # case sensitve
verbatim_coordinates_field = 'text2' # the field on the locality table used for verbatim coordinates (we don't use separate verbatimlat and verbatimlong fields, because life is too short)

# for testing only - the index of the records to start processing at so we can fix errors. Set to None to process all records.
start_at = None
break_at = 10000 # stop after this number to check in the db, also only for testing. Set to None otherwise


### SCRIPT ###

start = time.perf_counter()

# the db connection parameters
db = get_db(*credentials.values())

# we don't need to remember collectionobjects or co-attributes because every row is a collection object, same for dets
agents = {}
geographies = {}
localities = {}
collectingEvents = {}
collectingTrips = {}
taxa = {}
acceptedTaxa = {}
prepTypes = {} # not presently used as everything is one prep type (see prepTypeID)

llunit_enum = {
  'DD': 0,
  'DMS': 1,
  'DM': 2
}

counter = Counter(100)
exception = None
exception_table = None

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
    
    collectionObject = get_record_data(record, collectonObjectMapping)

    #we don't capture anything for empty collection objects
    if dict_is_empty(collectionObject):
      continue

    if not field_has_value('catalognumber', collectionObject):
      continue

    collectionObjectAttributes = get_record_data(record, collectionObjectAttributesMapping)

    determination = get_record_data(record, determinationMapping)
    
    locality = get_record_data(record, localityMapping)

    collectingEvent = get_record_data(record, collectingEventMapping)

    collectingEventAttributes = get_record_data(record, collectingEventAttributesMapping)

    # get / add the collecting agents
    collecting_agents = []
    collectorCount = 1 # because we start with collector 1
    while collectorCount <= maxCollectors:

      # collector fields are hardcoded here 
      # TODO add a mapping and use that instead
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

      collecting_agent['agenttype'] = 1 # all collectors are people, even if they're not...

      collecting_agents.append(collecting_agent)
      
      collectorCount += 1

    # get / add the determiner
    determiner = get_record_data(record, determinerMapping)

    project = get_record_data(record, collectingTripMapping)

    taxon = get_record_data(record, taxonMapping)

    geography = get_record_data(record, geographyMapping)

    # let's start adding stuff
    # note that we use tuples as dictionary keys for agents, etc
    for collecting_agent in collecting_agents:
      try:
        collecting_agent_id = find_or_add_record(db.agents, collecting_agent, agents, 'agentid')
        collecting_agent["agentid"] = collecting_agent_id
      except Exception as ex:
        exception = ex
        exception_table = 'agent'
        break


    # similar for determiner
    determiner_id = None
    if not dict_is_empty(determiner):
      determiner['agenttype'] = 1
      try: 
        determiner_id = find_or_add_record(db.agents, determiner, agents, 'agentid')
      except Exception as ex:
        exception = ex
        exception_table = 'agent'
        break

    geography_id = None
    locality_id = None
    event_id = None
    trip_id = None

    if not dict_is_empty(geography):
      try:
        geography_id = find_or_add_record(db.geography, geography, geographies, 'geographyid')
      except:
        exception = ex
        exception_table = 'geography'
        break

    locality['geographyid'] = geography_id
    if not dict_is_empty(locality):

      # required fields

      # this can happen if we have a country/province only
      if 'localityname' not in locality:
        locality['localityname'] = ' ' # we use an empty string just so there is a value
      
      locality['srclatlongunit'] = 0

      # sorting out these coordinates
      lat = record[dataset_latitude_field]
      long = record[dataset_longitude_field]
    
      if lat and not long:
        exception = 'missing long coordinate'
        exception_table = 'locality'
        break

      if long and not lat: 
        exception = 'missing lat coordinate'
        exception_table = 'locality'
        break

      if lat and long:
        verbatim_coords = f"{lat}, {long}"
        locality[verbatim_coordinates_field] = verbatim_coords
        converted = None
        try:
          converted = convert(verbatim_coords)
        except:
          pass

        if converted:
          standardized_coords = converted.to(converted.original_format).split(', ')
          locality['lat1text'] = standardized_coords[0]
          locality['long1text'] = standardized_coords[1]
          locality['latitude1'] = converted.decimal_latitude
          locality['longitude1'] = converted.decimal_longitude
          locality['srclatlongunit'] = llunit_enum[converted.original_format]

      try:
        locality_id = find_or_add_record(db.localities, locality, localities)
      except Exception as ex:
        exception = ex
        exception_table = 'locality'
        break

    collecting_event_attributes_id = None
    if not dict_is_empty(collectingEventAttributes): 
      try:
        collecting_event_attributes_id = db.collectingeventattributes.insert(collectingEventAttributes)
      except Exception as ex:
        exception = ex
        exception_table = 'collectingeventattributes'
        break

    if not dict_is_empty(project):
      try: 
        trip_id = find_or_add_record(db.collectingtrips, project, collectingTrips)
      except Exception as ex:
        exception = ex
        exception_table = 'project'
        break

    collectingEvent['localityid'] = locality_id
    collectingEvent['collectingtripid'] = trip_id
    collectingEvent['collectingeventattributeid'] = collecting_event_attributes_id

    if 'startdate' in collectingEvent and collectingEvent['startdate']:
      collectingEvent['startdateprecision'] = get_date_precision(collectingEvent['startdate'])
      collectingEvent['startdate'] = fix_date(collectingEvent['startdate'])
    else:
      collectingEvent['startdate'] = None

    if not dict_is_empty(collectingEvent):
      try:
        event_id = find_or_add_record(db.collectingevents, collectingEvent, collectingEvents)
      except Exception as ex:
        exception = ex
        exception_table = 'collecting event'
        break

    # we added the collecting agents, now we can add the collector records
    for index, collecting_agent in enumerate(collecting_agents):
      collector = {
        "agentid": collecting_agent["agentid"],
        "collectingeventid": event_id,
        "ordernumber": index,
        "isprimary": index == 0
      }

      collector_error = False
      try:
        db.collectors.insert(collector)
      except Exception as ex:
        exception = ex
        exception_table = 'collector'
        collector_error = True
        break

      if collector_error:
        break

    collection_object_attributes_id = None
    if not dict_is_empty(collectionObjectAttributes):
      try:
        collection_object_attributes_id = db.collectionobjectattributes.insert(collectionObjectAttributes)
      except Exception as ex:
        exception = ex
        exception_table = 'collectingobjectattributes'
        break

    collectionobject_id = None
    collectionObject['collectingeventid'] = event_id
    collectionObject['collectionobjectattributeid'] = collection_object_attributes_id
    try:
      collectionobject_id = db.collectionobjects.insert(collectionObject)
    except Exception as ex:
      exception = ex
      exception_table = 'collection object'
      break

    # we can only add determination if there is a taxon
    if dict_is_empty(taxon) and not dict_is_empty(determination):
      exception_table = 'determination'
      exception = 'missing taxon name'
      break

    taxon_id = None
    if not dict_is_empty(taxon):
      try:
        taxon_id = find_or_add_record(db.taxa, taxon, taxa, 'taxonid')
      except Exception as ex:
        exception = ex
        exception_table = 'taxon'
        break

      # we need the accepted taxon
      if taxon_id not in acceptedTaxa:
        acceptedTaxa[taxon_id] =  taxon_id
        try:
          taxon_record = db.taxa.find({'taxonid': taxon_id})[0]
          while field_has_value('acceptedid', taxon_record):
            taxon_record = db.taxa.find({'taxonid': taxon_record['acceptedid']})[0]
          acceptedTaxa[taxon_id] = taxon_record['taxonid']
        except Exception as ex:
          exception = ex
          exception_table = 'accepted taxon'
          break

      accepted_id = acceptedTaxa[taxon_id]

      determination['taxonid'] = taxon_id
      determination['preferredtaxonid'] = accepted_id
      determination['determinerid'] = determiner_id

      # fix the date
      if 'determineddate' in determination and determination['determineddate']:
        determination['determineddateprecision'] = get_date_precision(determination['determineddate'])
        determination['determineddate'] = fix_date(determination['determineddate'])
      else:
        determination['determineddate'] = None

      if not dict_is_empty(determination):
        try: 
          determination['collectionobjectid'] = collectionobject_id
          determination['IsCurrent'] = True
          db.determinations.insert(determination)
        except Exception as ex:
          error = True
          exception_table = 'determination'
          break

    # we always add a preparation
    preparation = {
      "collectionobjectid": collectionobject_id, 
      "preptypeid": prepTypeID,
      "countamt": 1 # vials only, so one vial per collectionboject
    }

    try:
      db.preparations.insert(preparation)
    except Exception as ex:
      exception = ex
      exception_table = 'preparation'
      break

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

end = time.perf_counter()
elapsed = end - start
hours, remainder = divmod(elapsed, 3600)
minutes, seconds = divmod(remainder, 60)

# if testing set a value exception in the debug console here to invoke the rollback 
if exception:
  print('error with', exception_table, 'for record', collectionObject['catalognumber'], f'(row number {counter.count})')
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
  print('all done!')