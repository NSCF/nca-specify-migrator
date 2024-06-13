from utils.timestamp import get_timestamp
from utils.field_has_value import field_has_value

class Determination:

  def __init__(self, cursor, collectionid) -> None:

    if not collectionid:
      raise Exception('collectionid is required')
    
    self.cursor = cursor
    self.collectionid = collectionid

  def insert(self, detdata):

    if not detdata or not isinstance(detdata, dict) or len(detdata.keys()) == 0:
      raise Exception('detdata dictionary is required')
    
    # required fields
    if not field_has_value('collectionObjectID', detdata):
      raise Exception('collectionObjectID is required')
    
    if not field_has_value('taxonID', detdata):
      raise Exception('taxonID is required')
    
    if not field_has_value('preferredTaxonID', detdata):
      raise Exception('preferredTaxonID is required')
    
    sql = 'INSERT INTO determination '
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
      determination_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return determination_id