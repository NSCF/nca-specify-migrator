import csv, sys
from os import path
from db import get_db

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')
missing_authors = []

results = db.taxon.find({"fullname": "araneae"})
if len(results) == 1:
  araneae = results[0]
else:
  print('Oops, something went wrong fetching Araneae from the database!')
  exit()

def getSpeciesWithNoAuthor(db, taxon, family, list):
  taxon['family'] = family
  if taxon['rank'].lower() == 'family':
    family = taxon['name']

  if taxon['rank'].lower() in ['species', 'subspecies']:
    if not taxon['author'] or not taxon['author'].strip():
      del taxon['name']
      del taxon['author']
      del taxon['rank']
      list.append(taxon)
  
  children = db.taxon.find({"parentid" : taxon['taxonID']})
  for child in children:
    getSpeciesWithNoAuthor(db, child, family, list)

print('getting records without authors')
getSpeciesWithNoAuthor(db, araneae, None, missing_authors)

db.close()

if len(missing_authors) > 0:
  print('There are', len(missing_authors), 'taxa missing author info.')
  print('Writing to file')
  with open('missingauthors.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=missing_authors[0].keys(), )
    writer.writeheader()
    writer.writerows(missing_authors)
    print('all done!')  
else:
  print('All names have authors')
