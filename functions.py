import sys, re, csv
from os import path
from db.utils.field_has_value import field_has_value

# function must take the dbtable and the node as first two arguments, then any additional arguments it needs
def walkdowntree(dbtable, idfield, start_node, function, *args):
  try:
    function(dbtable, start_node, *args)
  except Exception as ex:
    raise ex
  children = dbtable.find({"parentid" : start_node[idfield]})
  for child in children:
    try:
      walkdowntree(dbtable, idfield, child, function, *args)
    except Exception as ex:
      raise ex

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

def get_record_data(record, mapping):
  data = {}
  for record_field, specify_field in mapping.items():
      if field_has_value(record_field, record): 
        data[specify_field] = record[record_field]
  return data

def find_or_add_record(dbtable, record, records_dict, idfield=None):

  record_tuple = tuple(record.values())
  record_id = None
  if record_tuple in records_dict:
    record_id = records_dict[record_tuple]
  else:
    if hasattr(dbtable,'find') and callable(dbtable.find): # apparently this is how we check for methods...
      try:
        dbrecords = dbtable.find(record)
      except Exception as ex:
        raise ex

      if len(dbrecords) > 0:
        record_id = dbrecords[0][idfield]
        records_dict[record_tuple] = record_id
    
    if not record_id and hasattr(dbtable,'insert'):
      try:
        record_id = dbtable.insert(record)
        records_dict[record_tuple] = record_id
      except Exception as ex:
        raise ex

  return record_id

# 1 is day, 2 is month, 3 is year  
def get_date_precision(date):
  precision = None
  if date and isinstance(date, str) and date.strip():
    date_parts = re.split(r'[-\/]', date)
    last_part = date_parts.pop()
    while len(date_parts) and int(last_part) == 0:
      last_part = date_parts.pop()
    
    if int(last_part) > 0:
      date_parts.append(last_part)
      precision = 4 - len(date_parts)
    
  return precision

def fix_date(date):
  if date:
    if '/' in date:
      date = date.replace(r'/', '-')
  
    if date and '-00' in date:
      date = date.replace('-00', '-01')

  return date