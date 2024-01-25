from sqlalchemy import create_engine, inspect,text
import yaml
import pandas as pd
import psycopg2
import time


class DatabaseConnector:
    
    def __init__(self):
        
        self.credentials_reader =''
        self.tables_names =''
        
    
    def read_db_creds(self,db_credentials)-> dict:
        '''
        This function reads the credentials stored in a file
        
        Parameters:
        ---------------
        db_credentials: database cresndials file
        
        Return:
        ------------
        Return the dictionary of credentials stored in a file
        
        ''' 
           
        with open(db_credentials, 'r') as credentials:
            
            credentials_reader = yaml.safe_load(credentials)
        if True:
            print("CREDENTIALS APPROVED")
            
        return credentials_reader

    
    def init_db_engine(
        self,
        credentials:str,
        database_type:str,
        database_api:str
        ):
        
        '''
        This function create and initiate a database engine.
        Parameters:
        ------------------
        credentials:  String referencing the user credentials
        database_type: Type of Database to use
        database_api:  database API
        
        Return:
        Rteurn an sql engine with the given credentials
        
        '''
        print("INITIATING DATABASE...")
        time.sleep(1)
        db_credentials = self.read_db_creds(credentials)
        credentials_list = []
        
        credentials_data = list(db_credentials.values())
        
        for data in credentials_data:
            credentials_list.append(data)
        
        RDS_HOST = credentials_list[0]
        RDS_PASSWORD= credentials_list[1]
        RDS_USER = credentials_list[2]
        RDS_DATABASE = credentials_list[3]
        RDS_PORT = credentials_list[4]
        DATABASE_TYPE = database_type
        DBAPI = database_api
        
        engine = create_engine(
            f"{DATABASE_TYPE}+{DBAPI}://
           {RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:
            {RDS_PORT}/{RDS_DATABASE}"
           )
        
        if True:
            time.sleep(1)
            print("DATABASE ENGINE SUCCESSFULLY INITIATED")
           
        return engine

    def list_db_tables(
        self,
        user_credential:dict,
        database_type:str,
        database_api:str
        ):
        
        '''
        This function list all database tables using the engine created
        from init_db_engine function.
        
        Parameters:
        --------------
        user_credential: The user token to access and initiate the database engine
        database_type: Type of Database to use
        database_api:  database API
        See also https://wiki.python.org/moin/UsingDbApiWithPostgres for more information 
        about database API
        
        Return:
        -----------
        Return a list of names of tables available in the database
        '''
    
        db_connection = self.init_db_engine(user_credential,database_type,database_api)
        inspector = inspect(db_connection)
        tables_names = inspector.get_table_names()
        time.sleep(1)
        print("AVAILABLE TABLES IN DATABASE")
        print(tables_names)
        return tables_names
    
    
    def upload_to_db(
        self,
        host:str,
        user:str,
        password:str,
        database_name:str,
        port:int,
        user_data:str,
        table_name:str
        ):
        
        '''
        This function initiate the a connection to a database server and transfer data to a given database table
        
        Parameters:
        -----------------
        host: Networking address of the database server.
        User: Database User connection username
        Password: Database user connection password
        Database_name: The name of the database to access
        Port: Database Server port number 
        user_data: The data to upload to the database
        Table_name: Name of database table to store data
        '''
        postgres_str = f'postgresql://{user}:{password}@{host}:{port}/{database_name}'
        connection = create_engine(postgres_str)
        
        user_data.to_sql(table_name,con=connection,index=False)
        return user_data
        
    
    

