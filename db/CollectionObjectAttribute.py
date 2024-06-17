from .utils.timestamp import get_timestamp

class CollectionObjectAttribute:

  def __init__(self, cursor, collectionid):

    if not cursor:
      raise Exception('cursor is required')
    
    if not collectionid:
      raise Exception('disciplineid is required')
    
    self.cursor = cursor
    self.collectionid = collectionid

  def insert(self, objectattributedata):

    if not objectattributedata or not isinstance(objectattributedata, dict) or len(objectattributedata.keys()) == 0:
      raise Exception('objectattributedata dictionary is required')
    
    sql = 'INSERT INTO collectionobjectattribute '
    fields = []
    values = []

    for key, val in objectattributedata.items():
      if isinstance(val, str) and val.strip().lower() == 'null':
        val = None
      fields.append(key)
      values.append(val)

    # all the other fields
    fields.append('collectionmemberid')
    values.append(self.collectionid)

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
      collectionobjectattribute_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return collectionobjectattribute_id