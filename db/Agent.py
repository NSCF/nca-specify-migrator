# note that this assumes all agents are people and not institutions

from .utils.timestamp import get_timestamp

class Agent:

  def __init__(self, cursor, divisionid) -> None:
    
    if not cursor:
      raise Exception('cursor is required')
    
    if not divisionid:
      raise Exception('divisionid is required')
    
    
    self.cursor = cursor
    self.divisionid = divisionid


  def find(self, criteria):

    if not criteria or not isinstance(criteria, dict) or len(criteria.keys()) == 0:
      raise Exception('criteria dictionary is required')
    
    params = [self.divisionid]
    sql = '''select agentid, firstname, lastname, initials, title from agent
      where divisionid = %s
    '''

    clauses = []
    for key, val in criteria.items():
      
      if isinstance(val, list):
        parts = []
        for param in val:
          parts.append(key + " = %s" )
          clauses.append(' OR '.join(parts))
          params.append(param)
      else:
          clauses.append(key + " = %s")
          params.append(val)
    
    sql += ' AND ' + ' AND '.join(clauses)

    try:
      self.cursor.execute(sql, params)
      results = self.cursor.fetchall()
    except Exception as ex:
      raise ex
  
    return results

  def insert(self, agentdata):

    if not agentdata or not isinstance(agentdata, dict) or len(agentdata.keys()) == 0:
      raise Exception('agentdata dictionary is required')

    sql = 'INSERT INTO agent '
    fields = []
    values = []

    for key, val in agentdata.items():
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
      agent_id = self.cursor.lastrowid
    except Exception as ex:
      raise ex
    
    return agent_id