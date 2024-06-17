# check all taxa in a dataset are in the database
# alerts if authors are needed to resolve homonyms
# note however that this is not a perfect solution for homonyms as one author can create homonyms in the same year. In these cases we need identifiers.

import csv, sys
from os import path
from db.db import get_db
from mappings import taxon, geography

check = 'taxa' # taxa or geography
csvFile = r'NCA-data-export-20220901-OpenRefine-ie-edits20240313-coll12345Added-OpenRefine_20240617.csv'
csvDir = r'D:\NSCF Data WG\Specify migration\ARC PHP\NCA\SPECIFY\DATA'


### SCRIPT ###

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

print('reading file...')
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()

mapping = taxon
dbtable = db.taxa
if check == 'geography':
  mapping = geography
  dbtable = db.geography

found_names = set()
not_found = set()
duplicate_names = set()
counter = 0
with open(path.join(csvDir, csvFile), 'r', encoding="utf8", errors='ignore') as f:
  for row in csv.DictReader(f):

    dataset_record = {}
    for datasetfield, databasefield in mapping.items():
      val = row[datasetfield]
      if val:
        if isinstance(val, str):
          dataset_record[databasefield] = val.strip() #it has to be a string or None
      else:
        dataset_record[databasefield] = None

    dataset_record_tuple = tuple(dataset_record.values()) #so that we can do searches like below
    if dataset_record_tuple not in found_names and dataset_record_tuple not in not_found and dataset_record_tuple not in duplicate_names:
      db_records = dbtable.find( dataset_record )
      if len(db_records) > 1:
        duplicate_names.add(dataset_record_tuple)
      elif len(db_records) == 1:
        found_names.add(dataset_record_tuple)
      else:
        not_found.add(dataset_record_tuple)
  
    counter += 1
    if counter % 100 == 0:
      print(counter, 'records processed', end='\r')

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

db.close()

if len(not_found) > 0:
  print('The following names are not in the database:')
  not_found = list(not_found)
  not_found.sort()
  for name in not_found:
    print(' | '.join(list(name)))
  
  print()
  print('Please add these names or correct in the dataset before proceeding with the migration')

if len(duplicate_names) > 0:
  duplicate_names = list(duplicate_names)
  duplicate_names.sort()
  print('The follow names are duplicated in the database:')
  for name in duplicate_names:
    print(' | '.join(list(name)))
  
  print()
  print('Check higher classification or authors and identifiers to disambiguate these names, then run this script again')

if not len(not_found) and not len(duplicate_names) :
  print('All', check, 'names verified, you can proceed with migration.')
   

