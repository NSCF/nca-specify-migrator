# The DB interface
# I decided to go with raw SQL over SQLAlchemy as that just seems to be a thin wrapper over SQL anyway, without much apparent benefit

import mysql.connector
from Taxon import Taxon
from User import User
from Agent import Agent
from Locality import Locality

    
class DB:
  def __init__(self, conn, cursor, taxon, user):
    self.connection = conn
    self.cursor = cursor
    self.taxon = taxon
    self.user = user

  def rollback(self):
    self.connection.rollback()

  def commit(self):
    self.connection.commit()

  def close(self):
    self.cursor.close()
    self.connection.close()

  # just in case
  def __del__(self):
    self.rollback()
    self.close()

def get_db(user, password, host, database, collectionname):
  try:
    connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
  except:
    raise Exception('could not connect to database')

  #get the collection
  sql = 'select collectionid, disciplineid from collection where collectionname = %s'
  cursor = connection.cursor(dictionary=True)
  try:
    cursor.execute(sql, (collectionname,))
    collections = cursor.fetchall()
  except Exception as ex:
    raise ex
  
  if len(collections) == 0:
    raise Exception('No collection named ' + collectionname)
  
  collectionid = collections[0]['collectionid']
  disciplineid = collections[0]['disciplineid']
  
  taxon = Taxon(cursor, disciplineid)
  user = User(cursor)

  db = DB(connection, cursor, taxon, user)
  
  return db
  