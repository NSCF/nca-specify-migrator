### The import script
### REMEMBER TO MAKE A BACKUP OF YOUR DATABASE BEFORE YOU DO THIS!
### Make sure to run check_taxa_or_geography first so that this doesn't stop part way when missing taxa are found (for both taxa and geography)
### Also run check_coordinates first to fix any errors there so we don't have problems here

import csv, sys
from os import path
from db.db import get_db
from db.utils.field_has_value import field_has_value
from db.utils.dict_is_empty import dict_is_empty
from Counter import Counter
from functions import find_or_add_record, get_date_precision
from mappings import collectonObjectMapping, collectionObjectAttributesMapping, \
   determinationMapping, localityMapping, collectingEventMapping, \
  collectingEventAttributesMapping

# clone the coords parser repo from https://github.com/NSCF/geo-coords-parser-python
# this may have to change depending where you put the repo
sys.path.append(r"C:\devprojects\geo-coords-parser-python")
from converter import convert


csvFile = r'NCA-data-export-20220901-OpenRefine-ie-edits20240313-coll12345Added-OpenRefine_20240621.csv'
csvDir = r'D:\NSCF Data WG\Specify migration\ARC PHP\NCA\SPECIFY\DATA'
maxCollectors = 5 # the maximum number of collectors for any record in the dataset
prepTypeID = 21 # wet specimen/s

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

### SCRIPT ###

# we don't need to remember collectionobjects or co-attributes because every row is a collection object, same for dets
agents = {}
geographies = {}
localities = {}
collectingEvents = {}
collectingTrips = {}
taxa = {}
acceptedTaxa = {}
prepTypes = {} # not presently used as everything is one prep type (see prepTypeID)

counter = Counter(100)
exception = None
exception_table = None
with open(path.join(csvDir, csvFile), 'r', encoding="utf8", errors='ignore') as f:
  reader = csv.DictReader(f)
  for record in reader:
    
    collectionObject = {}
    for record_field, specify_field in collectonObjectMapping.items():
      collectionObject[specify_field] = record[record_field]

    #we don't capture anything for empty collection objects
    if dict_is_empty(collectionObject):
      continue

    if not field_has_value('catalognumber', collectionObject):
      continue

    collectionObjectAttributes = {}
    for record_field, specify_field in collectionObjectAttributesMapping.items():
      collectionObjectAttributes[specify_field] = record[record_field]

    determination = {}
    for record_field, specify_field in determinationMapping.items():
      determination[specify_field] = record[record_field]
    
    locality = {}
    for record_field, specify_field in localityMapping.items():
      locality[specify_field] = record[record_field]

    collectingEvent = {}
    for record_field, specify_field in collectingEventMapping.items():
      collectingEvent[specify_field] = record[record_field]

    collectingEventAttributes = {}
    for record_field, specify_field in collectingEventAttributesMapping.items():
      collectingEventAttributes[specify_field] = record[record_field]

    # get / add the collectors
    collectors = []
    collectorCount = 1 # because we start with collector 1
    while collectorCount <= maxCollectors:

      collector = {
        "title": record['coll' + str(collectorCount) + "title"],
        "firstname":  record['coll' + str(collectorCount) + "firstName"],
        "lastname": record['coll' + str(collectorCount) + "lastName"],
        "initials": record['coll' + str(collectorCount) + "initials"]
      }

      if dict_is_empty(collector):
        break

      collector['agenttype'] = 1 # all collectors are people, even if they're not...

      collectors.append(collector)
      
      collectorCount += 1

    # get / add the determiner
    determiner = {
      "firstname": record["DetBy_FirstName"],
      "lastname": record["DetBy_LastName"],
      "initials": record["DetBy_Initials"]
    }

    project = {
      "collectingtripname": record["Project"]
    }

    taxon = {
      "fullname": record["fullname"],
      "rank": record["rank"]
    }

    geography = {
      "name": record["geography"],
      "rank": record["geographyRank"]
    }

    # let's start adding stuff
    # note that we use tuples as dictionary keys
    collectorIDs = []
    for collector in collectors:
      try:
        collector_id = find_or_add_record(db.agents, collector, agents, 'agentid')
      except Exception as ex:
        exception = ex
        exception_table = 'collector'
        break

      collectorIDs.append(collector_id)

    # similar for determiner
    determiner_id = None
    if not dict_is_empty(determiner):
      determiner['agenttype'] = 1
      try: 
        determiner_id = find_or_add_record(db.agents, determiner, agents, 'agentid')
      except Exception as ex:
        exception = ex
        exception_table = 'determiner'
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

      # TODO add decimal coordinates and srclatlongunit (0 == DD or null, 1 = everything else)
      if field_has_value(locality['verbatimlatitude']) and not field_has_value(locality['verbatimlongitude']):
        exception = 'missing long coordinate'
        exception_table = 'locality'
        break

      if field_has_value(locality['verbatimlongitude']) and not field_has_value(locality['verbatimlatitude']): 
        exception = 'missing lat coordinate'
        exception_table = 'locality'
        break



      try:
        locality_id = find_or_add_record(db.localities, locality, localities)
      except Exception as ex:
        exception = ex
        exception_table = 'locality'
        break

    collecting_event_attributes_id = None
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

    if collectingEvent['startdate']:
      collectingEvent['startdateprecision'] = get_date_precision(collectingEvent['startdate'])

    if not dict_is_empty(collectingEvent):
      try:
        event_id = find_or_add_record(db.collectingevents, collectingEvent, collectingEvents)
      except Exception as ex:
        exception = ex
        exception_table = 'collecting event'
        break

    collection_object_attributes_id = None
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
      db.collectionobjects.insert(collectionObject)
    except Exception as ex:
      exception = ex
      exception_table = 'collection object'
      break

    taxon_id = None
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
        while field_has_value(taxon_record['acceptedid']):
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
    if determination['determineddate']:
      determination['determineddateprecision'] = get_date_precision(determination['determineddate'])

    if not dict_is_empty(determination):
      try: 
        determination['collectionobjectid'] = collectionobject_id
        determination['IsCurrent'] = True
        db.determinations.insert(determination)
      except Exception as ex:
        error = True
        exception_table = 'determination'
        break

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

    counter += 1

if exception:
  print('error with', exception_table, 'for record', collectionObject['catalognumber'])
  print(str(exception))
  db.rollback()
  db.close()
else:
  db.commit()
  db.close()
  print('all done!')