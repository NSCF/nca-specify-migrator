# The DB interface
# I decided to go with raw SQL over SQLAlchemy as that just seems to be a thin wrapper over SQL anyway, without much apparent benefit

import mysql.connector

class Taxon:

  def __init__(self, connection, disciplineid):

    if not connection or not disciplineid:
      raise Exception('connection and disciplineid are required')
    
    self.connection = connection
    self.disciplineid = disciplineid

  # criteria can be fields in the taxon table and/or 'rank'    
  def find(self, criteria):
    
    params = [self.disciplineid]
    sql = '''select t.taxonID, t.name, t.author, t.fullname from taxon t 
      join taxontreedefitem ttdi on t.taxontreedefitemid = ttdi.taxontreedefitemid
      join discipline d on d.taxontreedefid = ttdi.taxontreedefid
      where d.disciplineid = %s
    '''

    if criteria: 
      
      if not isinstance(criteria, dict):
        raise Exception('selection criteria must be a dictionary')
      else:
        if 'rank' in criteria:
          if criteria['rank'].strip():
            sql += ' AND ' + 'ttdi.name = %s'
            params.append(criteria['rank'].strip())
          del criteria['rank']
        
        if len(criteria.keys()) > 0:
          sql += ' AND ' + ' AND '.join([f"{key} = %s" for key in criteria])
          params += criteria.values()

    cursor = self.connection.cursor(dictionary=True)
    try:
      cursor.execute(sql, params)
      results = cursor.fetchall()
      cursor.close()
      return results
    except Exception as ex:
      cursor.close()
      raise ex
    
class DB:
  def __init__(self, taxon):
    self.taxon = taxon

def get_db(user, password, host, database, collectionname):
  connection = mysql.connector.connect(host=host, user=user, password=password, database=database)

  #get the collection
  sql = 'select disciplineid from collection where collectionname = %s'
  cursor = connection.cursor(dictionary=True)
  try:
    cursor.execute(sql, (collectionname,))
    collections = cursor.fetchall()
  except Exception as ex:
    raise ex
  
  if len(collections) == 0:
    raise Exception('No collection named ' + collectionname)
  
  collection = collections[0]
  
  taxon = Taxon(connection, collection['disciplineid'])

  db = DB(taxon)
  return db
  