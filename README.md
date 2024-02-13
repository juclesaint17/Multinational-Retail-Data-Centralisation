# Multinational Retail Data Centralisation
# Table of Contents
1. [Description](#description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Database_Utils](#database_utils)
      - [4.1 Read_db_creds ](read_db_creds-4.1)
      - [4.2 Init_db_engine](init_db_engine-4.2)
      - [4.3 List_db_tables](list_db_tables-4.3)
      - [4.4 Upload_to_db](upload_to_db-4.4)
## Description
Multinational Retail Data Centralisation is a data-driven application system,it collect data
from different data sources,analyse and clean data and store it in a database.
The database will act as a centralised location where data can be accessed for multi tasks processing.
The purpose of building this project is to facilitate retails companies with sales data spread across different data source 
to access and analyse the data easily.
While extracting and analysing data coming from different sources, data have different formats and,
some of them contains missing values, inconsistent that make the data very difficult to be processed easily.To make data more consistent and available to end users we proceed to the steps below to clean data.
- Import Pandas library
- read data with Pandas classes
- Clean the data using Pandas functions and python functions
- Store the cleaned data a to a centralised database for business data processing
## Installation
## Usage
this is us
## Database_utils:
Before starting the process,we first create a python file named database_utils, and added a class on it.The class inside the python file DatabaseConnector will be use to initiate a connection to the database with the credentials given in yaml file, list available tables to perform data cleaning and a function to store the data to a centralised database.Below is the print screen and description of each function in the Database Connector class.
#### 4.1 Read_db_creds:
   This function accept as argument a dictionary of the user credentials and it is used to initiate a connecton with the database as shown below.
   ```
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
        def read_db_creds(self,db_credentials)-> dict:
        '''
        This function reads the credentials stored in a file
        
        Parameters:
        ---------------
        db_credentials: database credentials file
        
        Return:
        ------------
        Return the dictionary of credentials stored in a file
        
        ''' 
        try:
               
            with open(db_credentials, 'r') as credentials:
                
                credentials_reader = yaml.safe_load(credentials)
            if True:
                print("CREDENTIALS APPROVED")
                
            return credentials_reader
        except Exception as error:
            print("Please check your CREDENTIALS or Contact Admin\n",error)

   ```


#### 4.2.Init_db_engine:
This function take as arguments the user credentials,the database type and the database API to create a connection to the database where the data are stored in tables.
The function reads the user credentials by calling the read_db_creds() function, and use it to initiate a connection to the database engine.
The figure below illustrates how the function is defined.

```
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
        
        try:
            
            engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
            if True:
                time.sleep(1)
                print("DATABASE ENGINE SUCCESSFULLY INITIATED")
            
            return engine
        except:
            print("DATABASE ENGINE INITIATiON FAILED")
```

#### 4.3. List_db_tables:
This function takes as arguments the user credentials to initiate a connection to the database engine with the corresponding database API and database type,by calling the init_db_engine() function,and return all the available tables in the database.
the screenshot below shows how the function is defined

```
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
    
        
```
#### 4.4. Upload_to_db:
After collecting and cleaning data, the function is used to upload the data to the centralised database for data processing.It takes as arguments the created database user credentials like username,password,network address,the name of new database to store the cleaned data,and the table name where the data will be stored, as shown below

```
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
```




