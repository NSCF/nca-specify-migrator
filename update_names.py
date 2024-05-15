# a basic script to update species names in the database from an extract of the WSC (because sometimes we don't have authors or IDs)
import path, sys
import csv
from time import time
import mysql.connector
from db import get_db

csv_dir = r'C:\devprojects\wsc-species-extractor'
csv_file = r'wsc-species-and-synonyms-20240510.csv'

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

### SCRIPT ###
# for each name in the database look it up in the list and update if only one, otherwise report that it must be updated manually

#we need two indexes
ids_index = {}
names_index = {}

print('building indexes from data')
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()
counter = 0
with open(path.join(csv_dir, csv_file), 'r', encoding="utf8", errors='ignore') as f:
  for row in csv.DictReader(f):
    name = row['name']
    id = row['id']
    ids_index[id] = row 

    # we have to cater for homonyms
    if name in names_index:
      names_index[name] = None # use this as a flag for homonyms
    else:
      names_index[name] = row 
    
    counter += 1
    if counter % 1000 == 0:
      print(counter, 'records processed', end='\r')

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

print('updating names in database...')
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()
counter = 0

db.taxon.find({'rank': ['species', 'subspecies']})



