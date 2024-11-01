# Taxon must only work with records for this discipline
class Taxon:

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
    sql = '''select t.taxonid, t.acceptedid, t.name, t.author, t.fullname, ttdi.name as rank, t.guid, 
      at.fullname as acceptedname, at.author as acceptednameauthor, at.guid as acceptednameguid from taxon t 
      join taxontreedefitem ttdi on t.taxontreedefitemid = ttdi.taxontreedefitemid
      join discipline d on d.taxontreedefid = ttdi.taxontreedefid
      left outer join taxon at on at.taxonid = t.acceptedID
      where d.disciplineid = %s
    '''

    if criteria: 
      
      if not isinstance(criteria, dict):
        raise Exception('selection criteria must be a dictionary')
      else:
        
        # expects that strings will be strings and other values will be the relevant type
        clauses = []
        for key in criteria.keys():
          if key.lower() == 'rank':
            field = 'ttdi.name'
          else:
            field = 't.' + key
          
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

      return results
    except Exception as ex:
      raise ex
    
  def update(self, taxonid, params):

    if not params or not isinstance(params, dict) or len(params.keys()) == 0:
      raise Exception('params dict is required for update')
    
    sql = 'update taxon set '
    clauses = []
    values = []
    for key, val in params.items():
      if isinstance(val, str) and val.strip().lower() == 'null':
        val = None
      clause = key + ' = %s'
      clauses.append(clause)
      values.append(val)
      
    sql += ', '.join(clauses)
    sql += ' where taxonid = %s'
    values.append(taxonid)

    try:
      self.cursor.execute(sql, values)
    except Exception as ex:
      raise ex