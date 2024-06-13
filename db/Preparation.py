from utils.timestamp import get_timestamp
from utils.field_has_value import field_has_value

class Preparation:

  def __init__(self, cursor, collectionid) -> None:

    if not cursor:
      raise Exception('cursor is required')

    if not collectionid:
      raise Exception('collectionid is required')
    
    self.connection = cursor
    self.collectionid = collectionid

    self.data = None

  def insert(self, prepdata):

    if not prepdata or not isinstance(prepdata, dict) or len(prepdata.keys()) == 0:
      raise Exception('prepdata dictionary is required')
    

    #required fields

    if not field_has_value('collectionObjectID', prepdata):
      raise Exception('collectionObjectID is required')

    if not field_has_value('prepTypeID', prepdata):
      raise Exception('prepTypeID is required')
    
    sql = 'INSERT INTO preparation '
    fields = []
    values = []

    for key, val in self.data.items():
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
      preparation_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return preparation_id