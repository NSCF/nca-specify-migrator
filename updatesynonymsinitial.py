# update synonyms using an extract provided by the WSC. Intended for initial updates only, all subsequent updates should be made
# using the API (see updatesynonyms)

from os import path
import sys
import csv
from datetime import datetime
from time import time
from db import get_db
from functions import walkdowntree

username = 'ian' # the name of the user indicate as 'modifiedby'
csv_dir = r'D:\NSCF Data WG\Specify migration\ARC PHP\NCA\SPECIFY\DATA'
names_file = r'WSC-V24.5-20240115-AcceptedNames-OpenRefine.csv'
synonyms_file = r'WSC-V24.5-20240115-Synonyms-OpenRefine.csv'

db_params = { "user": "root", "pwd": "root", "host": "localhost", "database": "specify_php", "collection": "arachnida"}

### SCRIPT ###

def dtnow():
  return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

db = get_db(*db_params.values())

# first validate the user
users = db.user.find({ "username": username})
if len(users) == 0:
  print('Oops, no user named ' + username +'! Please fix and try again...')
  exit()

if len(users) > 1:
  print('Oops! There is more than one user named ' + username + '. This should not happen, please fix your database...')
  exit()

user = users[0]

names_index = {}
names_ids_index = {}
synonyms_index = {}

print('building names index...')
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()
counter = 0
with open(path.join(csv_dir, names_file), 'r', encoding="utf8", errors='ignore') as f:
  for row in csv.DictReader(f):
    record = {
      "taxonID": row["ID"], 
      "family": row["Family"],
      "genus": row["Genus"],
      "specificEpithet": row["Species"],
      "infraspecificEpithet": row["InfraSpecies"],
      "fullname": row["fullname"],
      "author": row["AuthorString"],
      "wsc_distribution": row["Distribution"],
      "wsc_url": row["SpeciesURL"],
      "guid": row["GSDTaxonGUID"]
    }

    if row["fullname"] in names_index:
      names_index[row["fullname"]].append(record)
    else:
      names_index[row["fullname"]] = [record]
    names_ids_index[record["taxonID"]] = record

    counter += 1
    if counter % 1000 == 0:
      print(counter, 'records processed', end='\r')

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

print('building synonyms index...')
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()
counter = 0
with open(path.join(csv_dir, synonyms_file), 'r', encoding="utf8", errors='ignore') as f:
  for row in csv.DictReader(f):
    record = {
      "taxonID": row["ID"], 
      "acceptedID": row["AcceptedName_ID"],
      "genus": row["Genus"],
      "specificEpithet": row["Species"],
      "infraspecificEpithet": row["InfraSpecies"],
      "fullname": row["fullname"],
      "author": row["AuthorString"],
      "guid": row["GSDTaxonGUID"]
    }

    if row["fullname"] in synonyms_index:
      synonyms_index[row["fullname"]].append(record)
    else:
      synonyms_index[row["fullname"]] = [record]

    counter += 1
    if counter % 1000 == 0:
      print(counter, 'records processed', end='\r')

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()
print()


# we only traverse the spiders
results = db.taxon.find({"fullname": "araneae"})
if len(results) == 1:
  araneae = results[0]
else:
  print('Oops, something went wrong fetching Araneae from the database!')
  exit()

def update_synonyms(db_table, db_taxon, names_ids_index, synonyms_index, updates, problems, duplicate_names, duplicate_guids, names_to_add, manual_check, counter):
  if db_taxon['rank'].lower() in ['species', 'subspecies']:
    
    name_and_author = db_taxon['fullname'].strip()
    if db_taxon['author'] and db_taxon['author'].strip():
      name_and_author += ' ' + db_taxon['author'].strip()

    if db_taxon['fullname'] in synonyms_index:
      # there may be more than one matching synonym so we need to resolve
      options = synonyms_index[db_taxon['fullname']]
      matching_synonyms = [s for s in options if s['guid'] == db_taxon['guid'] or s['author'] == db_taxon['author']]
      if len(matching_synonyms) > 0:
        synonym = matching_synonyms[0]
        synonym_and_author = synonym["fullname"]
        if synonym["author"] and synonym["author"].strip():
          synonym_and_author += ' ' + synonym["author"].strip()
        accepted_name = names_ids_index[synonym["acceptedID"]]
        if not accepted_name:
          problems.append(synonym_and_author)
          return
        
        # it may already have the accepted name
        if accepted_name['fullname'] == db_taxon['acceptedname'] and accepted_name["author"] == db_taxon["acceptednameauthor"]:
          return
        
        #otherwise we need to update
        db_accepted_names = []
        accepted_name_with_author = accepted_name["fullname"]
        if accepted_name['author'] and accepted_name['author'].strip():
          accepted_name_with_author += ' ' + accepted_name['author'].strip()

        # try by GUID first
        if accepted_name["guid"]: #this should always be true
          db_accepted_names = db_table.find({"guid": accepted_name["guid"]}) # they might not have guids
          if len(db_accepted_names) > 1:
            duplicate_guids.append(accepted_name["guid"])
            return

        # else try by name
        if len(db_accepted_names) == 0:
          db_accepted_names = db_table.find({"fullname": accepted_name["fullname"], 'author': accepted_name['author']})
          if len(db_accepted_names) > 1:
            duplicate_names.append(accepted_name_with_author)
            return

        # we may still not have a name if it's not in the database
        if len(db_accepted_names) == 0:
          names_to_add.append(accepted_name_with_author)
          return
        
        # otherwise update the synonymy
        # accepted_name_id = db_accepted_names[0]["taxonID"]
        # db_table.update(db_taxon, {"acceptedid": accepted_name_id, "timestampmodified": dtnow(), "ModifiedByAgentID": user['agentID']})
        updates[synonym_and_author] = name_and_author # for testing
        counter.tickupdates()
    
    # not in the synonyms index, but may be indicated as a synonym. These we need to check manually because the synonyms are not complete
    else:
      if db_taxon['acceptedname'] and db_taxon['acceptedname'].strip():
        
        acceptedname_and_author = db_taxon['acceptedname']
        if db_taxon['acceptednameauthor'] and db_taxon['acceptednameauthor'].strip():
          acceptedname_and_author += ' ' + db_taxon['acceptednameauthor'].strip()

        manual_check[name_and_author] = acceptedname_and_author

    counter.ticktotal()

  return

class Counter:
  def __init__(self, print_increment) -> None:
    self.total = 0
    self.updates = 0
    self.print_increment = print_increment

  def ticktotal(self):
    self.total += 1
    if self.total % self.print_increment == 0:
      print(self.total, 'records processed', end='\r')

  def tickupdates(self):
    self.updates += 1
    

updates = {}
problems = []
duplicate_names = []
duplicate_guids = []
names_to_add = []
manual_check = {}
counter = Counter(10)

print("updating synonyms")
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()

walkdowntree(db.taxon, 'taxonID', araneae, update_synonyms, names_ids_index, synonyms_index, updates, problems, duplicate_names, duplicate_guids, names_to_add, manual_check, counter)

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

print()
print(counter.total, 'names were checked')

print()
if len(updates.keys()) > 0:
  print('The following names were synonymized:')
  for [key, value] in updates.items():
    print(key, '=>', value)
else:
  print("No names were synonymized")

if len(names_to_add) > 0:
  print()
  print('The following names need to be added to the database:')
  for name in names_to_add:
    print(name)
  print()
  print('Add these names and then run the script again.')

if len(problems) > 0:
  print()
  print("There are problems in the WSC dataset for the following names:")
  for prob in problems:
    print(prob)

if len(duplicate_names) > 0:
  print()
  print('The following names are duplicated in the database. Please fix these and run the script again:')
  for name in duplicate_names:
    print(name)

if len(duplicate_guids) > 0:
  print()
  print('The following GUIDs appear for more than one name in the database. Please look up the names with each GUID and correct:')
  for guid in duplicate_guids:
    print(guid)

if len(manual_check.keys()) > 0:
  print()
  print('The following synonyms exist in the database but need to be verified manually as they may predate the WSC data:')
  for [key, val] in manual_check.items():
    print(key, '=>', val)

print("That's it, all done...")