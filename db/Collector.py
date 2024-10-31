from .utils.timestamp import get_timestamp
from .utils.field_has_value import field_has_value

class Collector:
  
  def __init__(self, cursor, divisionid) -> None:

    if not cursor:
      raise Exception('cursor is required')
    
    if not divisionid:
      raise Exception('divisionid is required')
    
    self.cursor = cursor
    self.divisionid = divisionid


  def insert(self, collectordata):

    if not collectordata or not isinstance(collectordata, dict) or len(collectordata.keys()) == 0:
      raise Exception('collectordata dictionary is required')
    
    # required fields
    if not field_has_value('agentid', collectordata):
      raise Exception('agentid is required')
    
    if not field_has_value('collectingeventid', collectordata):
      raise Exception('collectingeventid is required')
    
    if not field_has_value('ordernumber', collectordata):
      raise Exception('ordernumber is required')
    
    if not field_has_value('isprimary', collectordata):
      raise Exception('isprimary value is required')

    sql = 'INSERT INTO collector '
    fields = []
    values = []

    for key, val in collectordata.items():
      if isinstance(val, str) and val.strip().lower() == 'null':
        val = None
      fields.append(key)
      values.append(val)

    # all the other fields
    fields.append('divisionid')
    values.append(self.divisionid)

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
      collector_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return collector_id

    