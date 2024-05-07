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

discipline = 'Arachnology'
collection = 'Arachnida'

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

class DBTable:
    def __init__(self, engine, table):
        self.engine = engine
        self.table = table

    def find(self, search_criteria):
        query = self.table.select()
        if search_criteria:
            conditions = [self.table.c[key] == value for key, value in search_criteria.items()]
            query = query.where(and_(*conditions))

        records = []
        with Session(engine) as session:
            for record in session.execute(query).all():
                records.append(record)
        return records

class TaxonTable:
    def __init__(self, engine):
        self.engine = engine
        self.table = metadata.tables['taxon']

    def find(self, search_criteria):
        query = self.table.select()
        query = query.select_from(
            self.table
            .join(metadata.tables['taxontreedef'])
            .join(metadata.tables['discipline'], 
                _and() # this is where I gave up, it's just easier to use SQL, since this is basically just SQL!
            )
        )
        if search_criteria:
            conditions = [self.table.c[key] == value for key, value in search_criteria.items()]
            query = query.where(and_(*conditions))

        query = query.join(metadata.tables['taxontreedef']).join(metadata.tables['discipline'])
        query = query.where(metadata.tables['discipline'].c.Name == discipline)

        records = []
        with Session(engine) as session:
            for record in session.execute(query).all():
                records.append(record)
        return records


    
db = {}
#add the necessary database operations
for tablename in metadata.tables:
    if tablename == 'taxon':
        db[tablename] = TaxonTable(engine)
    else:
        db[tablename] = DBTable(engine, metadata.tables[tablename])
   
    



  

