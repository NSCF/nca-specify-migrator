# because in older Specify databases we have offical country names, 'province of ...', etc
# see README.md for the source of the countries data

import sys, csv
from functions import walkdowntree
from Counter import Counter
from db.db import get_db

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

print('reading countries file')
countries = {}
with open('official-names-of-countries-2024.csv', 'r', encoding="utf8", errors='ignore') as f:
  for row in csv.DictReader(f):
    full_name = row['officialName']
    short_name = row['country']
    countries[full_name] = short_name

def fix_geography(geotable, geo_record, countries_dict, record_counter, update_counter):
  geo_name = geo_record['name']
  updates = None
  if geo_name in countries_dict:
    if countries_dict[geo_name] != geo_name:
      updates = {'name': countries_dict[geo_name], 'fullname': countries_dict[geo_name]}
  elif 'Province of' in geo_name:
    geo_name = geo_name.replace('Province of', '').strip()
    updates = { 'name': geo_name, 'fullname': geo_name }
  
  if updates:
    geotable.update(geo_record['geographyID'], updates)
    update_counter.increment()

  record_counter.increment()

# get started
print('updating geography names...')


results = db.geography.find({'name': 'Earth'})

if len(results) != 1:
  print('Oops! Couldn\'t find root node for geography. Please try again...')
  exit()

record_counter = Counter(100)
update_counter = Counter()

sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()

try:
  walkdowntree(db.geography,'geographyID', results[0], fix_geography, countries, record_counter, update_counter)
except Exception as ex:
  db.rollback()
  print("There was an error:")
  print(str(ex))
  exit()

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

db.commit()
db.close()
print()
print(update_counter.count, 'records updated')
print('All done!')
  




  

