from .utils.timestamp import get_timestamp

class Locality:

  def __init__(self, cursor, disciplineid) -> None:

    if not cursor:
      raise Exception('cursor is required')
    
    if not disciplineid:
      raise Exception('disciplineid is required')
    
    self.cursor = cursor
    self.disciplineid = disciplineid

  def insert(self, localitydata):

    if not localitydata or not isinstance(localitydata, dict) or len(localitydata.keys()) == 0:
      raise Exception('localitydata dictionary is required')
    
    sql = 'INSERT INTO locality '
    fields = []
    values = []

    for key, val in localitydata.items():
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
      locality_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return locality_id

  