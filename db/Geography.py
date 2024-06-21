# Taxon must only work with records for this discipline
class Geography:

  def __init__(self, cursor, disciplineid):

    if not cursor:
      raise Exception('cursor is required')
    
    if not disciplineid:
      raise Exception('disciplineid is required')
    
    self.cursor = cursor
    self.disciplineid = disciplineid

  # criteria can be fields in the taxon table and/or 'rank'    
  def find(self, criteria):

    if not criteria or not isinstance(criteria, dict) or len(criteria.keys()) == 0:
      raise Exception('criteria dictionary is required')
    
    params = [self.disciplineid]
    sql = '''select g.geographyid, g.name, gtdi.name as rank from geography g
      join geographytreedefitem gtdi on g.geographytreedefitemid = gtdi.geographytreedefitemid
      join discipline d on d.geographytreedefid = gtdi.geographytreedefid
      where d.disciplineid = %s
    '''
        
    clauses = []
    for key in criteria.keys():
      if key.lower() == 'rank':
        field = 'gtdi.name'
      else:
        field = 'g.' + key
      
      if isinstance(criteria[key], list):
        parts = []
        for param in criteria[key]:
          parts.append(field + " = %s" )
          clauses.append(' OR '.join(parts))
          params.append(param)
      else:
          clauses.append(field + " = %s")
          params.append(criteria[key])
    sql += ' AND ' + ' AND '.join(clauses)

    try:
      self.cursor.execute(sql, params)
      results = self.cursor.fetchall()
    except Exception as ex:
      raise ex
  
    return results
  

  def update(self, geographyid, params):

    if not params or not isinstance(params, dict) or len(params.keys()) == 0:
      raise Exception('params dict is required for update')
    
    sql = 'update geography set '
    clauses = []
    values = []
    for key, val in params.items():
      if isinstance(val, str) and val.strip().lower() == 'null':
        val = None
      clause = key + ' = %s'
      clauses.append(clause)
      values.append(val)
      
    sql += ', '.join(clauses)
    sql += ' where geographyid = %s'
    values.append(geographyid)

    try:
      self.cursor.execute(sql, values)
    except Exception as ex:
      raise ex