# Check coordinates in a potential import file before importing.
# Outputs a new csv file to the same dir as the import file with catalog numbers and problematic coordinates. 
# This can be used to fix errors (best with facets in OpenRefine) and make updates back to the original file.
# Make sure catalog number field has been checked and only have unique values before doing this

import csv, sys, re
from os import path
sys.path.append(r"C:\devprojects\geo-coords-parser-python")
from converter import convert
from mappings import collectonObjectMapping, localityMapping
from Counter import Counter
from db.utils.field_has_value import field_has_value
from db.utils.dict_is_empty import dict_is_empty

csvFile = r'NCA-data-export-20220901-OpenRefine-ie-edits20240313-coll12345Added-OpenRefine_20240621.csv'
csvDir = r'D:\NSCF Data WG\Specify migration\ARC PHP\NCA\SPECIFY\DATA'

### SCRIPT ###
localities = {}
counter = Counter(100)
print('processing data file...')
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()
print()

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

    locality = {}
    for record_field, specify_field in localityMapping.items():
      locality[specify_field] = record[record_field]

    # skip any without coordinates
    if not field_has_value('verbatimlatitude', locality) and not field_has_value('verbatimlongitude', locality):
      continue

    locality_tuple = tuple(locality.values())
    if locality_tuple in localities:
      localities[locality_tuple]['catnums'].append(collectionObject['catalognumber'])
    else:

      # if we have one we need the other
      if (field_has_value('verbatimlatitude', locality) and not field_has_value('verbatimlongitude', locality)) or \
      field_has_value('verbatimlongitude', locality) and not field_has_value('verbatimlatitude', locality):
        localities[locality_tuple] = {
          'locality': locality['localityname'], # assuming this field will have something
          'lat': locality['verbatimlatitude'],
          'long': locality['verbatimlongitude'],
          'error': 'missing coordinate',
          'catnums': [collectionObject['catalognumber']]
        }
        continue
      
      # else
      try:
        convert(f"{locality['verbatimlatitude']}, {locality['verbatimlongitude']}")
      except Exception as ex:
        localities[locality_tuple] = {
          'locality': locality['localityname'], # assuming this field will have something
          'lat': locality['verbatimlatitude'],
          'long': locality['verbatimlongitude'],
          'error': str(ex),
          'catnums': [collectionObject['catalognumber']]
        }

    counter.increment()

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

print('writing out results file')
records = []

#flatten the errors object
for err in localities.values():
  for catnum in err['catnums']:
    record = {
      "catnum": catnum,
      'locality': err['locality'], # assuming this field will have something
      'edited': None,
      'lat': err['lat'],
      'long': err['long'],
      'error': err['error']
    }
    records.append(record)

with open(path.join(csvDir, 'coordinate_errors.csv'), 'w', encoding='utf8', newline='') as csvfile:
  fieldnames = records[0].keys()
  writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
  writer.writeheader()
  writer.writerows(records)

print('all done!')



    
      
