# The provides the ORM models for the database used in the import script


from sqlalchemy import create_engine, MetaData, select, and_
from sqlalchemy.orm import Session, Mapped, relationship

### PARAMS ###

# the tables to operate on
table_names = [
    "agent", 
    'taxon',
    'geography',
    'locality',
    'geocoorddetail',
    'collector',
    'collectingevent',
    "collectionobject", 
    'collectionobjectattribute',
    'preparation',
    'determination', 
    'determiner'
    ]

# MySQL connection details
username = 'root'
password = 'root'
host = 'localhost'
port = '3306'  
database = 'specify_php'

### SCRIPT ###

DATABASE_URL = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

engine = create_engine(DATABASE_URL, echo=False)  # 'echo' is optional for debugging

metadata = MetaData()
try:
    metadata.reflect(engine, only=table_names)
except Exception as ex:
    print(str(ex))
    exit()

# pull them all out so we can use them
Agent = metadata.tables['agent']
Collector = metadata.tables['collector']
CollectingEvent = metadata.tables['collectingevent']
CollectionObject = metadata.tables['collectionobject']
Collection = metadata.tables['collection']

Collector.agent = relationship(Agent)
CollectingEvent.collectors = relationship(Collector)
CollectionObject.collectingevent = relationship(CollectingEvent)
CollectionObject.collection = relationship(Collection)

with Session(engine) as session:
    co = session.query(CollectionObject).first()
    co.collection
    i = 0

class AgentTable:
    def __init__(self, engine):
        self.engine = engine
        self.table = table

    def find(self, search_criteria):
        query = (
            select(Agent)
            .join(Collector.agent)
            .join(CollectionObject.collector)
            .join(Collection.collectionobject)
        )
        if search_criteria:
            conditions = [self.table.c[key] == value for key, value in search_criteria.items()]
            query = query.where(and_(*conditions))

        records = []
        with Session(engine) as session:
            for record in session.execute(query).all():
                records.append(record)
        return records

class DBTable:
    def __init__(self, engine, table):
        self.engine = engine
        self.table = table

    def find(self, search_criteria):
        query = select(self.table)
        if search_criteria:
            conditions = [self.table.c[key] == value for key, value in search_criteria.items()]
            query = query.where(and_(*conditions))

        records = []
        with Session(engine) as session:
            for record in session.execute(query).all():
                records.append(record)
        return records
    
db = {}
#add the necessary database operations
for tablename in metadata.tables:
    table = metadata.tables[tablename]
    db[tablename] = DBTable(engine, table)
   
    



  

