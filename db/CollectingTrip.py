from .utils.timestamp import get_timestamp
from .utils.field_has_value import field_has_value

class CollectingTrip:
  
  def __init__(self, cursor, disciplineid) -> None:

    if not cursor:
      raise Exception('cursor is required')
    
    if not disciplineid:
      raise Exception('disciplineid is required')
    
    self.cursor = cursor
    self.disciplineid = disciplineid


  def insert(self, tripdata):

    if not tripdata or not isinstance(tripdata, dict) or len(tripdata.keys()) == 0:
      raise Exception('tripdata dictionary is required')
    
    # required fields
    if not field_has_value('collectingtripname', tripdata):
      raise Exception('trip name is required')

    sql = 'INSERT INTO collectingtrip '
    fields = []
    values = []

    for key, val in tripdata.items():
      if isinstance(val, str) and val.strip().lower() == 'null':
        val = None
      fields.append(key)
      values.append(val)

    # all the other fields
    fields.append('disciplineid')
    values.append(self.disciplineid)

    fields.append('version')
    values.append(1)

    now = get_timestamp()
    fields.append('timestampcreated')
    values.append(now)
    fields.append('timestampmodified')
    values.append(now)

    sql += '(' + ', '.join(fields) + ') VALUES (' + ', '.join(['%s'] * len(values)) + ')'
    
    try:
      self.cursor.execute(sql, values)
      collectingtrip_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return collectingtrip_id

    