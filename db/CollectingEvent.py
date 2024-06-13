from utils.timestamp import get_timestamp
from utils.field_has_value import field_has_value

class CollectingEvent:
  
  def __init__(self, cursor, disciplineid) -> None:

    if not cursor:
      raise Exception('cursor is required')
    
    if not disciplineid:
      raise Exception('disciplineid is required')
    
    self.connection = cursor
    self.disciplineid = disciplineid

    self.data = None

  def insert(self, eventdata):

    if not eventdata or not isinstance(eventdata, dict) or len(eventdata.keys()) == 0:
      raise Exception('eventdata dictionary is required')
    
    # required fields
    if not field_has_value('localityID', eventdata):
      raise Exception('localityID is required')
    
    self.data = eventdata

    

    sql = 'INSERT INTO collectingevent '
    fields = []
    values = []

    for key, val in self.data.items():
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
    fields.append('timestampcreateed')
    values.append(now)
    fields.append('timestampmodified')
    values.append(now)

    sql += '(' + ', '.join(fields) + ') VALUES (' + ['%s'] * len(values) + ')'
    
    try:
      self.cursor.execute(sql, values)
      collectingevent_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return collectingevent_id

    