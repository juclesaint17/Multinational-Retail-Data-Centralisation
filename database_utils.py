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
        This function reads the credentials stored in a YAML file
        
        Parameters:
        ---------------
        db_credentials: database credentials file
        
        Return:
        ------------
        Return the dictionary of credentials stored in a file
        
        ''' 
        try:
            print("\tLOADING CREDENTIALS...")   
            with open(db_credentials, 'r') as credentials:
                
                credentials_reader = yaml.safe_load(credentials)
            time.sleep(1)
            if True:
                print("\tCREDENTIALS SUCCESSFULLY LOADED")
                
            return credentials_reader
        except Exception as error:
            print("Please check your CREDENTIALS or Contact Admin\n",error)
    
    def init_db_engine(
        self,
        credentials:str,
        database_type:str,
        database_api:str
        ):
        '''
        This function create a connection with the database engine.
        Parameters:
        ------------------
        credentials:  String referencing the user credentials
        database_type: Type of Database to use
        database_api:  database API
        
        Return:
        Return an sql engine with the given credentials
        
        '''
        
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
        
        try:
            print("\tINITIATING DATABASE ENGINE...")
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
            if True:
                time.sleep(1)
                print("DATABASE ENGINE SUCCESSFULLY INITIATED")
                print("\t  STARTING PROCESS...")
                time.sleep(1)
            
            return engine
        except:
            print("DATABASE ENGINE INITIATiON FAILED")

    def list_db_tables(
        self,
        user_credential:str,
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
        try:
            
            db_connection = self.init_db_engine(user_credential,database_type,database_api)
            inspector = inspect(db_connection)
            tables_names = inspector.get_table_names()
            time.sleep(1)
            print("AVAILABLE TABLES IN DATABASE")
            #print(tables_names)
            return tables_names
        except Exception as error:
            print('COULD NOT CONNECT TO THE DATABASE SERVER\n',error)
            
        
        
    
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
        connected = False
        try:
            print("\tCONNECTING TO USER DATABASE")
            postgres_user = f'postgresql://{user}:{password}@{host}:{port}/{database_name}'
            connected = True
        except Exception as error:
            print("CONNECTION TO THE DATABASE FAILED")
            
        if connected: 
            print("SYSTEM CONNECTED TO USER DATABASE")
            connection = create_engine(postgres_user)
            time.sleep(1)
            try:
                print(f"\tUPLOADING DATA TO THE USER DATABASE TABLE {table_name}")
                user_data.to_sql(table_name,con=connection,index=False)
                if True:
                    print(f"\tDATA SUCCESSFULLY UPLOADED TO THE {table_name} DATABASE TABLE OF {database_name} DATABASE")
                    return user_data
            except Exception as error:
                print(f"UPLOAD TO THE TABLE :{table_name} FAILED")
                
    