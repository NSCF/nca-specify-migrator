import sys
from os import path
import csv

def walkdowntree(dbtable, idfield, start_node, function, *args):
  function(dbtable, start_node, *args)
  children = dbtable.find({"parentid" : start_node[idfield]})
  for child in children:
    walkdowntree(dbtable, idfield, child, function, *args)

def build_names_indexes(csv_dir, csv_file, fullname_field, author_field, id_field):
  
  sys.stdout.write('\033[?25l') #to clear the console cursor for printing progress
  sys.stdout.flush()
  counter = 0

  names_index = {} #<name, id> index
  ids_index = {} # <id, name> index
  duplicates = {}
  file = path.join(csv_dir, csv_file)
  if not path.exists(file):
    raise Exception('file does not exist')
  
  with open(file, 'r', encoding="utf8", errors='ignore') as f:
    for row in csv.DictReader(f):
      fullname = row[fullname_field]
      authorname = row[author_field]
      fullnameandauthor = fullname + ' '+ authorname
      ids_index[row[id_field]] = row
      if fullnameandauthor in names_index or fullnameandauthor in duplicates: 
        duplicates[fullnameandauthor] = None
        if fullnameandauthor in names_index:
          del names_index[fullnameandauthor]
      else:
        names_index[fullnameandauthor] = row

      counter += 1
      if counter % 100 == 0:
        print(counter, 'records processed', end='\r')

  sys.stdout.write('\033[?25h') #resetting the console cursor
  sys.stdout.flush()

  return {
    "names_index": names_index,
    "ids_index": ids_index,
    "duplicates": duplicates.keys()
  }
