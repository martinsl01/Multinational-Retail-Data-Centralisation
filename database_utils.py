import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd


class DatabaseConnector:
    def read_db_cred(self, filepath): # This to read data from 'db_cdreds.yaml'
        with open(filepath, 'r') as file:   
            db_creds = yaml.safe_load(file)
            return db_creds
        
    def init_db_engine(self, db_creds): 

        '''This method reads the return of the read_db_cred() method and
          it returns an sqlalchemy database engine.'''
        
        engine = create_engine(f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        engine.connect()
        return engine
    
    def list_db_tables(self, engine) -> None:
        engine.connect()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def read_pg_cred(self): # This to read data from 'pg_cdreds.yaml'
        with open('pg_creds.yaml', 'r') as creds:
            pg_creds = yaml.safe_load(creds)
            return pg_creds
        
    def connect_to_pg(self, pg_creds):

        '''This connects PostgreSQL and Python to allow direct upload to the database'''
        
        connect = create_engine(f"postgresql://{pg_creds['USER']}:{pg_creds['PASSWORD']}@{pg_creds['HOST']}:{pg_creds['PORT']}/{pg_creds['DATABASE']}")
        connect.connect()
        return connect
        
    
    def upload_to_db(self, products, connect) -> None:
        upload = products.to_sql('dim_date_times', connect)
        return upload 
    



 




