from .utils.timestamp import get_timestamp

class CollectingEventAttribute:

  def __init__(self, cursor, disciplineid):

    if not cursor:
      raise Exception('cursor is required')
    
    if not disciplineid:
      raise Exception('disciplineid is required')
    
    self.cursor = cursor
    self.disciplineid = disciplineid

  def insert(self, eventattributedata):

    if not eventattributedata or not isinstance(eventattributedata, dict) or len(eventattributedata.keys()) == 0:
      raise Exception('eventattributedata dictionary is required')
    
    sql = 'INSERT INTO collectingeventattribute '
    fields = []
    values = []

    for key, val in eventattributedata.items():
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
      collectingeventattribute_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return collectingeventattribute_id