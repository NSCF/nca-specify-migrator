# The DB interface
# I decided to go with raw SQL over SQLAlchemy as that just seems to be a thin wrapper over SQL anyway, without much apparent benefit
# There are also several idiosyncracies in the Specify database structure, such as requiring additional fields, which I wanted to 
# hard code and check for in queries to prevent errors

import mysql.connector
from .Agent import Agent
from .Collector import Collector
from .CollectingEvent import CollectingEvent
from .CollectingEventAttribute import CollectingEventAttribute
from .CollectingTrip import CollectingTrip
from .CollectionObject import CollectionObject
from .CollectionObjectAttribute import CollectionObjectAttribute
from .Determination import Determination
from .Locality import Locality
from .Preparation import Preparation
from .PrepType import PrepType
from .Taxon import Taxon
from .User import User
from .Geography import Geography

class DB:
  def __init__(self, conn, cursor, agent, collector, collectingevent, 
               collectingeventattribute, collectingtrip, collectionobject, collectionobjectattribute, determination,
                locality, preparation, preptype, taxon, user, geography):
    
    self.connection = conn
    self.cursor = cursor
    self.agents = agent
    self.collectors = collector
    self.collectingevents = collectingevent
    self.collectingeventattributes = collectingeventattribute
    self.collectingtrips = collectingtrip
    self.collectionobjects = collectionobject
    self.collectionobjectattributes = collectionobjectattribute
    self.determinations = determination
    self.localities = locality
    self.preparations = preparation
    self.preptypes = preptype
    self.taxa = taxon
    self.users = user
    self.geography = geography

  def rollback(self):
    self.connection.rollback()

  def commit(self):
    self.connection.commit()

  def close(self):
    self.cursor.close()
    self.connection.close()


def get_db(user, password, host, database, collectionname):
  try:
    connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
  except:
    raise Exception('could not connect to database')

  cursor = connection.cursor(dictionary=True)

  #get the collection
  sql = 'select collectionid, disciplineid from collection where collectionname = %s'
  try:
    cursor.execute(sql, (collectionname,))
    collections = cursor.fetchall()
  except Exception as ex:
    raise ex
  
  if len(collections) == 0:
    raise Exception('No collection named ' + collectionname)
  
  collectionid = collections[0]['collectionid']
  disciplineid = collections[0]['disciplineid']

  # get the division
  sql = 'select divisionid from discipline where disciplineid = %s'
  try:
    cursor.execute(sql, (disciplineid,))
    divisions = cursor.fetchall()
  except Exception as ex:
    raise ex
  
  divisionid = divisions[0]['divisionid']

  agent = Agent(cursor, divisionid)
  collector = Collector(cursor, divisionid)
  collectingEvent = CollectingEvent(cursor, disciplineid)
  collectingEventAttribute = CollectingEventAttribute(cursor, disciplineid)
  collectingTrip = CollectingTrip(cursor, disciplineid)
  collectionObject = CollectionObject(cursor, collectionid)
  collectionObjectAttribute = CollectionObjectAttribute(cursor, collectionid)
  determination = Determination(cursor, collectionid)
  locality = Locality(cursor, disciplineid)
  preparation = Preparation(cursor, collectionid)
  prepType = PrepType(cursor, collectionid)
  taxon = Taxon(cursor, disciplineid)
  user = User(cursor)
  geography = Geography(cursor, disciplineid)

  db = DB(connection, cursor, agent, collector, collectingEvent, 
               collectingEventAttribute, collectingTrip, collectionObject, collectionObjectAttribute, determination,
                locality, preparation, prepType, taxon, user, geography)
  
  return db
  