# Multinational Retail Data Centralisation
# Table of Contents
1. [Description](#description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Structure](#structure)
    - [4.a Database_utils.py](4.a-database_utils.py)
    - [4.b Data_extraction.py](4.b-data_extraction.py)
    - [4.c Data_cleaning.py](4.c-data_cleaning.py)
    - [4.d Data_processing.py](4.d-data_processing.py)  
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
For good use of this software,users should proceed to the following steps:
- The main use of the software is to clean data and upload it to a newly created database for future analysis and data manipulation,for this project first we need to install pgAdmin4 for the creation of the database ,and also install Visual Studio Code, a platform to build the software program.
As data will be download from different sources for cleaning and upload to another database for processing,
we need to import specifics library to perform those operations.Below is a list of libraries that must be install to run the software:
   - sqlalchemy
   - yaml
   - pandas
   - psycop2
   - time
   - tabula
   - json
   - requests
   - boto3
   - re
   - datetime
   - s3fs
   - numpy

If the user will save incoming data to a different storage location, we suggest installing and the OS library
and specify the path location where the data will be store and retrieved for data cleaning process.

## Usage


## Structure
First four python files are created:
- Database_utils.py
- Data_extraction.py
- Data_cleaning.py
- Data_processing.py
Database_utils.py file contains a python class named DatabaseConnector and,it is used to initiate a connection 
to the database,collect data stored in each table of the database.
Data_extraction.py contains a python class named DataExtractor, this file contains functions to extract data from different sources like:
   - Data stored in a database table
   - Retrieve data in tabula pdf format
   - Retrieve data in url json format
   - Data store in s3 bucket(AWS), CSV format
   - Data store in s3 bucket(AWS), JSON format
Data_cleaning.py file contains a python class called DataCleaning,and it is used to clean data retrieved from different sources,
and to upload the cleaned data to the database.
Data_processing.py file is the main file of our project,it is used to run the software program.

### 4.a Database_utils.py
This file contains a python class with functions to allow users to initiate a connection tothe database and a function to to upload cleaned data to the new database.
below is the list and description of each function within the  database_utils python file.
#### Read_db_creds:
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


#### Init_db_engine:
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

#### List_db_tables:
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
#### Upload_to_db:
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
### 4.b Data_extration.py
The data_extraction.py file is used to extract data from different sources.It comntains a python class defined
with functions to perform data extraction of different data sources.
Within the python class we have:
#### read_rds_table():
This function connect to the database, extract the tables and return the tables data as a pandas dataframe. 
The function takes as arguments:
 - table_name: 
   The database table to extract data
 - user_access:
    The user credentials used to initiate a connection to the database engine
 - database_type:
    Type of database used to access the tables
 - database_api:
    Application programming interface used to connect this application to the external database to extract data
The function returns data stored in the database table selected. Below is the screeshot of the read_rsd_table()
function andthe retsult of the script after testing it.

    ```
    def read_rds_table(
            self,
            table_name:str,
            user_access:str,
            database_type:str,
            database_api:str
            ):
            
            '''
            This function connect to the database, extract the tables and return the tables data as a pandas 
            dataframe. 
            
            Parameters:
            --------------
            table_name: 
            The database table to extract data
            user_access:
            The user credentials used to initiate a connection to the database engine
            database_type:
            Type of database used to access the tables
            database_api:
            Application programming interface used to connect this application to the external database to extract data
            
            Return:
            ----------
            Return a pandas Dataframe
            
            '''
            
            db_initialisation = DatabaseConnector()
            try:
                engine = db_initialisation.init_db_engine(user_access,database_type,database_api)
                with engine.connect() as conn:
                    user_data = pd.read_sql_table(table_name=table_name,con=conn)
                    print("\t LIST OF USERS DATA")
                    return user_data
            except Exception as error:
                print("FAILED TO CONNECT TO THE DATABASE...")
          
    ```
    ```
    user_credentials='db_creds.yaml' 
    database_type='postgresql'
    dbapi='psycopg2'
    host='localhost'
    user= 'postgres'
    password='Hermelan17'
    database = 'sales_data'
    port= 5432
        
        # Tables names to upload data
    user_table = 'dim_users'
    card_table = 'dim_card_details'
    store_table = 'dim_store_details'
    data_order_table ='orders_table'
    product_table = 'dim_products' 
    dates_table = 'dim_date_times' 
            
        # Tables names to retrieve data
    users_table = 'legacy_users'
    card_data = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'  
    end_point2 = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    tokens = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    store_csv_file = 'stores_data.csv'
    order_table = 'orders_table'
    s3_products = 's3://data-handling-public/products.csv'
    s3_dates = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'   
       
        
    tables_list = DataExtractor()
    print(tables_list.read_rds_table(table_name=users_table,user_access=user_credentials,database_type=database_type,database_api=dbapi))
    
    ```
    ```
    LOADING CREDENTIALS...
    CREDENTIALS SUCCESSFULLY LOAD
    DATABASE ENGINE SUCCESSFULLY INITIATED
    INITIATING DATABASE...
    LOADING CREDENTIALS...
    CREDENTIALS SUCCESSFULLY LOAD
    DATABASE ENGINE SUCCESSFULLY INITIATED
             LIST OF USERS DATA
           index first_name last_name date_of_birth                       company  ...         country country_code       phone_number   join_date                             user_uuid
    0          0   Sigfried     Noack    1990-09-30            Heydrich Junitz KG  ...         Germany           DE   +49(0) 047905356  2018-10-10  93caf182-e4e9-4c6e-bebb-60a1a9dcf9b8
    1          1        Guy     Allen    1940-12-01                       Fox Ltd  ...  United Kingdom           GB    (0161) 496 0674  2001-12-20  8fe96c3a-d62d-4eb5-b313-cf12d9126a49
    2          2      Harry  Lawrence    1995-08-02     Johnson, Jones and Harris  ...  United Kingdom           GB  +44(0)121 4960340  2016-12-16  fc461df4-b919-48b2-909e-55c95a03fe6b
    3          3     Darren   Hussain    1972-09-23                   Wheeler LLC  ...  United Kingdom           GB    (0306) 999 0871  2004-02-23  6104719f-ef14-4b09-bf04-fb0c4620acb0
    4          4      Garry     Stone    1952-12-20                    Warner Inc  ...  United Kingdom           GB      0121 496 0225  2006-09-01  9523a6d3-b2dd-4670-a51a-36aebc89f579
    ...      ...        ...       ...           ...
                 
    ```
The read_rds_table initiate a connection to the database engine by calling the init_db_engine() function of the 
DatabaseConector class store in Database_utils.py python file.Once connected, a pandas module to read sql table is used 
within the function to read and return data stored in the database table.

    
    
