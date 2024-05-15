# script for updating synonyms from an extract from WSC
# note to change the various field names used in the SCRIPT depending on the source...
import sys
from db import get_db
from functions import walkdowntree, build_names_indexes

db = get_db('root', 'root', 'localhost', 'specify_php', 'arachnida')

# need to run both ways - check synonyms on the list are in the tree, and synonyms in the tree are on the list (or not synonyms)

csvDir = r'C:\devprojects\wsc-species-extractor'
csvFile = r'wsc-species-and-synonyms-20240513.csv'

### SCRIPT ###

print('building indexes')
indexes = build_names_indexes(csvDir, csvFile, 'name', 'author', 'taxonID')
if len(indexes['duplicates']) > 0:
  print('the following taxa are duplicated in the data source and hence not considered:')
  for name in indexes['duplicates']:
    print(name)


# we only traverse the spiders
results = db.taxon.find({"fullname": "araneae"})
if len(results) == 1:
  araneae = results[0]
else:
  print('Oops, something went wrong fetching Araneae from the database!')
  exit()

def updateSynonyms(dbtable, dbtaxon, names_index, ids_index, to_add, duplicates, counter):
  if dbtaxon['rank'].lower() in ['species', 'subspecies']:

    dbtaxon_fullnameandauthor = (dbtaxon['fullname'].strip() + ' ' + dbtaxon['author'].strip()).strip()
    if dbtaxon_fullnameandauthor in names_index:
      wsc_taxon = names_index[dbtaxon_fullnameandauthor]

      # the simplest case, both are null, so nothing more needed
      if not dbtaxon['acceptedname'] and not wsc_taxon['acceptedNameID']:
        return

      if wsc_taxon['acceptedNameID']:
        wsc_accepted_taxon_id = wsc_taxon['acceptedNameID']
        wsc_accepted_taxon = ids_index[wsc_accepted_taxon_id]
        wsc_accepted_name = wsc_accepted_taxon['name']
        wsc_accepted_author = wsc_accepted_taxon['author']
        wsc_accepted_nameandauthor = (wsc_accepted_name.strip() + ' ' + wsc_accepted_author.strip()).strip()
        
        # check if we already have this synonym up to date
        if dbtaxon['acceptedname'] != wsc_accepted_name or dbtaxon['acceptednameauthor'] != wsc_accepted_author:
          # update the database
          accepted_db_candidates = dbtable.find({"fullname": wsc_accepted_name, "author": wsc_accepted_author})
          if len(accepted_db_candidates) == 0:
            to_add.append(wsc_accepted_nameandauthor)
          elif len(accepted_db_candidates) > 1:
            duplicates.append(wsc_accepted_nameandauthor)
          else:
            current_taxon = accepted_db_candidates[0]
            print(dbtaxon_fullnameandauthor, '==', wsc_accepted_nameandauthor)
            dbtable.update(dbtaxon, {"acceptedID": current_taxon["taxonID"]})
            counter.increment()
        return
      
      else:
        # this is now the case where wsc_taxon has no accepted name but dbtaxon does, so just update dbtaxon to null
        print(dbtaxon_fullnameandauthor, '!=', (dbtaxon['acceptedname'] + ' ' + dbtaxon['acceptednameauthor']).strip())
        # dbtable.update(dbtaxon, {"acceptedID": None})
        counter.increment()

to_add = []
duplicates = []

class Counter:
  def __init__(self, print_on):
    self.count = 0
    self.print_on = print_on

  def increment(self):
    self.count += 1
    if self.print_on:
      if self.count % self.print_on == 0:
        print(self.count, 'records processed')

counter = Counter(10)

print('updating synonyms')
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()
walkdowntree(db.taxon, 'taxonID', araneae, updateSynonyms, indexes['names_index'], indexes['ids_index'], db, counter)
sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()


db.close()
print(counter.count, 'synonyms updated')
print('all done')