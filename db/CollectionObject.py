from .utils.timestamp import get_timestamp
from .utils.field_has_value import field_has_value

class CollectionObject:

  def __init__(self, cursor, collectionid) -> None:

    if not cursor:
      raise Exception('cursor is required')
    
    if not collectionid:
      raise Exception('collectionid is required')
    
    self.cursor = cursor
    self.collectionid = collectionid

  def insert(self, objectdata):

    if not objectdata or not isinstance(objectdata, dict) or len(objectdata.keys()) == 0:
      raise Exception('objectdata dictionary is required')
    
    sql = 'INSERT INTO collectionobject '
    fields = []
    values = []

    for key, val in objectdata.items():
      if isinstance(val, str) and val.strip().lower() == 'null':
        val = None
      fields.append(key)
      values.append(val)

    fields.append('collectionid')
    values.append(self.collectionid)

    # this is required and always equal to the collectionid
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
      collectionobject_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return collectionobject_id
