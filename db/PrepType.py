class PrepType:

  def __init__(self, cursor, collectionid) -> None:

    if not cursor:
      raise Exception('cursor is required')
    
    if not collectionid:
      raise Exception('collectionid is required')
    
    self.cursor = cursor
    self.collectionid = collectionid

  def find(self, criteria):

    if not criteria or not isinstance(criteria, dict) or len(criteria.keys()) == 0:
      raise Exception('criteria dictionary is required')
    
    sql = '''select preptypeid, name as preptype from preptype
      where collectionid = %s
    '''
    params = [self.collectionid]


    clauses = []
    for key, val in criteria():
      clauses.append(key + " = %s")
      params.append(val)
    
    sql += ' AND ' + ' AND '.join(clauses)

    try:
      self.cursor.execute(sql, params)
      results = self.cursor.fetchall()
    except Exception as ex:
      raise ex
  
    return results




