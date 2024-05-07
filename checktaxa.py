# check all taxa in a dataset are in the database
# note the original version of this was written without taking author into consideration. This will need to be added in future versions to resolve homonyms
import csv, sys
from os import path
from db import get_db

csvFile = r'NCA-data-export-20220901-OpenRefine-ie-edits20240313-coll12345Added-OpenRefine_20240507.csv'
csvDir = r'D:\NSCF Data WG\Specify migration\ARC PHP\NCA\SPECIFY\DATA'
fullnamefield = 'fullname'


### SCRIPT ###

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

print('reading file...')
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()

found_names = set()
not_found = set()
counter = 0
with open(path.join(csvDir, csvFile), 'r', encoding="utf8", errors='ignore') as f:
  for row in csv.DictReader(f):
    fullname = row[fullnamefield].strip()
    if fullname and fullname not in found_names and fullname not in not_found:
      db_taxa = db.taxon.find( {'FullName': fullname} )
      if len(db_taxa) > 0:
        found_names.add(fullname)
      else:
        not_found.add(fullname)
  
    counter += 1
    if counter % 100 == 0:
      print(counter, 'records processed', end='\r')

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

if len(not_found) > 0:
  print('The follow names are not in the database:')
  for name in not_found:
    print(name)
  
  print()
  print('Please add these names or correct in the dataset before proceeding with the migration')
else:
  print('All names verified against the database, you can proceed with migration.')
   

