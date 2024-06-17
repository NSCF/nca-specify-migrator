# names in the database using the extract files sent by the WSC, so that we have the info we need to update synonymy later

from os import path
import sys
import csv
from datetime import datetime
from time import time
from db.db import get_db
from functions import walkdowntree
from .Counter import Counter

username = 'ian' # the name of the user to indicate as 'modifiedby'
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

def update_names(db_table, db_taxon, names_index, synonyms_index, updates, manual_check, counter):
  if db_taxon['rank'].lower() in ['species', 'subspecies']:
    
    name_and_author = db_taxon['fullname'].strip()
    if db_taxon['author'] and db_taxon['author'].strip():
      name_and_author += ' ' + db_taxon['author'].strip()

    update_values = {}

    if db_taxon['fullname'] in synonyms_index:
      # there may be more than one matching synonym so we need to resolve
      options = synonyms_index[db_taxon['fullname']]
      if len(options) > 1:
        match_as_list = [s for s in options if (db_taxon['guid'] and db_taxon['guid'] == s['guid']) or (db_taxon['author'] and db_taxon['author'] == s['author'])]
        synonym = match_as_list[0] # there can only be one
      else:
        synonym = options[0]

      if not db_taxon['author'] and synonym['author'] and synonym['author'].strip():
        update_values['author'] = synonym['author'].strip()

      if not db_taxon['guid'] and synonym['guid'] and synonym['guid'].strip():
        update_values['guid'] = synonym['guid'].strip()

      if db_taxon['author'] and db_taxon['author'].strip() and synonym['author'] and synonym['author'].strip() and \
         db_taxon['author'].strip() != synonym['author'].strip():
        manual_check.append(name_and_author)

    elif db_taxon['fullname'] in names_index:
      options = names_index[db_taxon['fullname']]
      if len(options) > 1:
        match_as_list = [s for s in options if (db_taxon['guid'] and db_taxon['guid'] == s['guid']) or (db_taxon['author'] and db_taxon['author'] == s['author'])]
        wsc_name = match_as_list[0] # there can only be one
      else:
        wsc_name = options[0]

      if not db_taxon['author'] and wsc_name['author'] and wsc_name['author'].strip():
        update_values['author'] = wsc_name['author'].strip()

      if not db_taxon['guid'] and wsc_name['guid'] and wsc_name['guid'].strip():
        update_values['guid'] = wsc_name['guid'].strip()

      if db_taxon['author'] and db_taxon['author'].strip() and wsc_name['author'] and wsc_name['author'].strip() and \
         db_taxon['author'].strip() != wsc_name['author'].strip():
        update_values['author'] = wsc_name['author'].strip()

    if (not db_taxon['author'] and 'author' not in update_values) or (not db_taxon['guid'] and 'guid' not in update_values):
      if name_and_author not in manual_check:
        manual_check.append(name_and_author)

    if len(update_values.keys()):
      db_table.update(db_taxon, {**updates, "timestampmodified": dtnow(), "ModifiedByAgentID": user['agentID']})
      updates.append({"name": name_and_author, "updated_author": update_values['author'] if 'author' in update_values else None, "updated_guid": update_values['guid'] if 'guid' in update_values else None})

    counter.increment()

  return  

updates = []
manual_check = []
counter = Counter(10)

print("updating names")
sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
sys.stdout.flush()

walkdowntree(db.taxon, 'taxonID', araneae, update_names, names_index, synonyms_index, updates, manual_check, counter)

sys.stdout.write('\033[?25h') #resetting the console cursor
sys.stdout.flush()

print()
print(counter.count, 'names were checked')

now = datetime.now().strftime("%Y%m%d%H%M%S")

if len(updates) > 0:
  print(len(updates), 'names were updated, writing updates file...')
  output_file_name = "updated_names_" + now + '.csv'
  with open(path.join(csv_dir, output_file_name), 'w', encoding='utf8', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=updates[0].keys())
    writer.writeheader()
    writer.writerows(updates)
else:
  print("No names were updated")

if len(manual_check) > 0:
  print(len(manual_check), 'names need to be updated manually, writing manual check file')
  output_file_name = "check_manually_" + now + '.csv'
  with open(path.join(csv_dir, output_file_name), 'w', encoding='utf8', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["name"])
    writer.writeheader()
    for name in manual_check:
      writer.writerow({"name": name})

print("All done...")