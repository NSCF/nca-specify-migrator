### The import script
### Make sure to run checktaxa first so that this doesn't stop part way when missing taxa are found
import csv
from os import path
from db.db import get_db
from db.utils.field_has_value import field_has_value
from db.utils.dict_is_empty import dict_is_empty
from Counter import Counter
from mappings import collectonObjectMapping, collectionObjectAttributesMapping, \
   determinationMapping, localityMapping, collectingEventMapping, \
  collectingEventAttributesMapping


csvFile = r''
csvDir = r''
maxCollectors = 5 # the maximum number of collectors for any record in the dataset
prepTypeID = 21 # wet specimen/s

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

### SCRIPT ###

# TODO add the date precision fields for all objects with date fields...

def find_or_add_record(dbtable, record, records_dict, idfield=None):
  record_tuple = tuple(record.values())
  record_id = None
  if record_tuple in records_dict:
    record_id = records_dict[record_tuple]
  else:
    if dbtable.find:
      try:
        dbrecords = dbtable.find(record)
      except Exception as ex:
        raise ex

      if len(dbrecords) > 0:
        record_id = dbrecords[0][idfield]
        records_dict[record_tuple] = record_id
    
    if not record_id:
      try:
        record_id = dbtable.insert(record)
        records_dict[record_tuple] = record_id
      except Exception as ex:
        raise ex

    return record_id

# we don't need to remember collectionobjects or co-attributes because every row is a collection object, same for dets
agents = {}
geographies = {}
localities = {}
collectingEvents = {}
collectingTrips = {}
taxa = {}
acceptedTaxa = {}
prepTypes = {} # not presently used as everything is one prep type (see prepTypeID)

error = False
with open(path.join(csvDir, csvFile), 'r', encoding="uft8", errors='ignore') as f:
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
    collectorCount = 0
    while collectorCount < maxCollectors:

      collector = {
        "title": record['coll' + collectorCount + "title"],
        "firstname":  record['coll' + collectorCount + "firstName"],
        "lastname": record['coll' + collectorCount + "lastName"],
        "initials": record['coll' + collectorCount + "initials"]
      }

      if dict_is_empty(collector):
        break

      collectors.append(collector)

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
        error = True
        print('error with collector for record', collectionObject['catalognumber'])
        print(collector)
        print(str(ex))
        print('fix in dataset and try again')
        break

      collectorIDs.append(collector_id)

    # similar for determiner
    determiner_id = None
    if not dict_is_empty(determiner):
      try: 
        determiner_id = find_or_add_record(db.agents, determiner, agents, 'agentid')
      except Exception as ex:
        error = True
        print('error with determiner for record', collectionObject['catalognumber'])
        print(determiner)
        print(str(ex))
        print('fix in dataset and try again')
        break

    geography_id = None
    locality_id = None
    event_id = None
    trip_id = None

    if not dict_is_empty(geography):
      try:
        geography_id = find_or_add_record(db.geography, geography, geographies)
      except:
        error = True
        print('error with locality for record', collectionObject['catalognumber'])
        print(str(ex))
        print('fix in dataset and try again')
        break

    locality['geographyid'] = geography_id
    if not dict_is_empty(locality):
      try:
        locality_id = find_or_add_record(db.localities, locality, localities)
      except Exception as ex:
        error = True
        print('error with locality for record', collectionObject['catalognumber'])
        print(str(ex))
        print('fix in dataset and try again')
        break

    if not dict_is_empty(project):
      try: 
        trip_id = find_or_add_record(db.collectingtrips, project, collectingTrips)
      except Exception as ex:
        error = True
        print('error with project for record', collectionObject['catalognumber'])
        print(str(ex))
        print('fix in dataset and try again')
        break

    collectingEvent['localityid'] = locality_id
    collectingEvent['collectingtripid'] = trip_id
    if not dict_is_empty(collectingEvent):
      try:
        event_id = find_or_add_record(db.collectingevents, collectingEvent, collectingEvents)
      except Exception as ex:
        error = True
        print('error with collecting event for record', collectionObject['catalognumber'])
        print(str(ex))
        print('fix in dataset and try again')
        break

    collectionobject_id = None
    collectionObject['collectingeventid'] = event_id
    try:
      db.collectionobjects.insert(collectionObject)
    except Exception as ex:
      error = True
      print('error with collection object for record', collectionObject['catalognumber'])
      print(str(ex))
      print('fix in dataset and try again')
      break

    taxon_id = None
    try:
      taxon_id = find_or_add_record(db.taxa, taxon, taxa, 'taxonid')
    except Exception as ex:
      error = True
      print('error with taxon for record', collectionObject['catalognumber'])
      print(str(ex))
      print('fix in dataset and try again')
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
        error = True
        print('error fetching accepted taxon record', collectionObject['catalognumber'])
        print(str(ex))
        break

    accepted_id = acceptedTaxa[taxon_id]

    determination['taxonid'] = taxon_id
    determination['preferredtaxonid'] = accepted_id
    determination['determinerid'] = determiner_id
    if not dict_is_empty():
      try: 
        determination['collectionobjectid'] = collectionobject_id
        determination['IsCurrent'] = True
        db.determinations.insert(determination)
      except Exception as ex:
        error = True
        print('error adding determination for record', collectionObject['catalognumber'])
        print(str(ex))
        break

      

      

if error:
  db.rollback()
  db.close()
else:
  db.commit()
  db.close()

print('all done!')


















