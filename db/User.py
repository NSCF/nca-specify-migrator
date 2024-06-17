class User:
  
  def __init__(self, cursor):

    if not cursor :
      raise Exception('cursor is required')
    
    self.cursor = cursor

  def find(self, criteria):

    if not criteria or not isinstance(criteria, dict) or len(criteria.keys()) == 0:
      raise Exception('criteria dictionary is required')

    sql = '''select u.specifyuserID, a.agentID, u.name, a.firstName, a.initials, a.lastName from specifyuser u
          join agent a on u.specifyuserid = a.specifyuserid
        '''
    
    clauses = []
    params = []
    for [key, value] in criteria.items():
      if value:
        if key.lower() == 'username':
          clauses.append('u.name = %s')
        else:
          clauses.append('a.' + key + ' = %s')
        params.append(value)
      
    if len(clauses) > 0:
      sql += ' WHERE ' + ' AND '.join(clauses)

    try:
      self.cursor.execute(sql, params)
      results = self.cursor.fetchall()
    except Exception as ex:
      raise ex
    
    return results