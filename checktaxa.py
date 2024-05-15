# check all taxa in a dataset are in the database
# note that we need authors in order to resolve homonyms

import csv, sys
from os import path
from db import get_db

csvFile = r'NCA-data-export-20220901-OpenRefine-ie-edits20240313-coll12345Added-OpenRefine_20240507.csv'
csvDir = r'D:\NSCF Data WG\Specify migration\ARC PHP\NCA\SPECIFY\DATA'
fullnamefield = 'fullname'
rankfield = 'rank'
authorfield = 'author'


### SCRIPT ###

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

print('reading file...')
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()

found_names = set()
not_found = set()
duplicate_names = set()
counter = 0
with open(path.join(csvDir, csvFile), 'r', encoding="utf8", errors='ignore') as f:
  for row in csv.DictReader(f):
    fullname = row[fullnamefield].strip()
    rank = row[rankfield].strip()
    author = row[authorfield].strip()
    fullnamewithauthor = (fullname + ' ' + author).strip()
    if fullnamewithauthor and fullnamewithauthor not in found_names and fullnamewithauthor not in not_found:
      db_taxa = db.taxon.find( {'fullname': fullname, 'author': author, 'rank': rank} )
      if len(db_taxa) > 1:
        duplicate_names.add(fullnamewithauthor)
      elif len(db_taxa) == 1:
        found_names.add(fullnamewithauthor)
      else:
        not_found.add(fullnamewithauthor)
  
    counter += 1
    if counter % 100 == 0:
      print(counter, 'records processed', end='\r')

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

db.close()

if len(not_found) > 0:
  print('The follow names are not in the database:')
  for name in not_found:
    print(name)
  
  print()
  print('Please add these names or correct in the dataset before proceeding with the migration')

if len(duplicate_names) > 0:
  print('The follow names are duplicated in the database:')
  for name in duplicate_names:
    print(name)
  
  print()
  print('Please fix these duplicates in the dataset before proceeding with the migration')

if len(found_names) == counter:
  print('All names verified against the database, you can proceed with migration.')
   

