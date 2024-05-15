# The DB interface
# I decided to go with raw SQL over SQLAlchemy as that just seems to be a thin wrapper over SQL anyway, without much apparent benefit

import mysql.connector

# Taxon must only work with records for this discipline
class Taxon:

  def __init__(self, connection, disciplineid):

    if not connection or not disciplineid:
      raise Exception('connection and disciplineid are required')
    
    self.connection = connection
    self.disciplineid = disciplineid

  # criteria can be fields in the taxon table and/or 'rank'    
  def find(self, criteria):
    
    params = [self.disciplineid]
    sql = '''select t.taxonID, t.name, t.author, t.fullname, ttdi.name as rank, t.guid, 
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

    cursor = self.connection.cursor(dictionary=True)
    try:
      cursor.execute(sql, params)
      results = cursor.fetchall()
      cursor.close()
      return results
    except Exception as ex:
      cursor.close()
      raise ex
    
  def update(self, taxon, params):

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
    sql += ' where taxonid = ' + taxon['taxonID']  

    cursor = self.connection.cursor()
    try:
      cursor.execute(sql, values)
      cursor.close()
    except Exception as ex:
      cursor.close()
      raise ex
    
class User:
  def __init__(self, connection):

    if not connection :
      raise Exception('connection is required')
    self.connection = connection

  def find(self, criteria):
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
  def __init__(self, conn, taxon, user):
    self.connection = conn
    self.taxon = taxon
    self.user = user

  def close(self):
    self.connection.close()


def get_db(user, password, host, database, collectionname):
  try:
    connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
  except:
    raise Exception('could not connect to database')

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
  user = User(connection)

  db = DB(connection, taxon, user)
  return db
  