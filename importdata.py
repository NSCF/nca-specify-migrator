### The import script
### Make sure to run checktaxa first so that this doesn't stop part way when missing taxa are found
import csv
from os import path
from db.db import get_db
from .mappings import collectonObjectMapping, collectionObjectAttributesMapping, \
   determinationMapping, localityMapping, collectingEventMapping, \
  collectingEventAttributesMapping

csvFile = r''
csvDir = r''
maxCollectors = 5

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

### SCRIPT ###

# we don't need collectionobjects or co-attributes because every row is a collection object, same for dets

prepTypeID = 21 # wet specimen/s

agents = []
agentIDs = []

localities = []
localityIDs = []

collectingEvents = []

prepTypes = []

with open(path.join(csvDir, csvFile), 'r', encoding="uft8", errors='ignore') as f:
  reader = csv.DictReader(f)
  for record in reader:
    
    collectionObject = {}
    for record_field, specify_field in collectonObjectMapping.items():
      collectionObject[specify_field] = record[record_field]

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

    # get the preptype

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

      collectors.append(collector)

    # get / add the determiner
    determiner = {
      "firstname": record["DetBy_FirstName"],
      "lastname": record["DetBy_LastName"],
      "initials": record["DetBy_Initials"]
    }

    taxonName = {
      "fullname": record["fullname"]
    }

    project = {
      "collectingtripname": record["Project"]
    }












