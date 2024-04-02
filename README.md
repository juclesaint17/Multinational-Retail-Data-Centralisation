
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
5. [Querying_Data](#querying_data) 
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
            Application programming interface used to connect this application to the external database to extract 
              data
            
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

#### retrieve_pdf_data():
this function is used to retrieve data stored in url link in pdf format and,used a tabula library to read the data retrieved.
It takes as argument the data URL link as shown below:

    ```
    def retrieve_pdf_data(self,data_url:str):       
            '''
            This function accept a tabular or PDF data url link and return
            a pandas dataframe
            Parameters:
            ---------------
            data_url: PDF file of the Tabular link
            
            Returns:
            -------
            Return data as dataframe.
            
            '''
            try:
                print("Reading all pages from pdf file")
                card_data = tabula.read_pdf(data_url, pages='all')
                print("PDF DATA COLLECTED...")
                return card_data
            except Exception as error:
                print("ERROR READING DATA")      
    ```
    ```
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
    #print(tables_list.read_rds_table(table_name=users_table,user_access=user_credentials,database_type=database_type,database_api=dbapi))
    
    pdf_data = tables_list.retrieve_pdf_data(data_url=card_data)
    print(pdf_data)
    ```
    ```
    18         572810170223       07/24                      Maestro             1997-11-19
    19  4763878823009700000       07/23                VISA 19 digit             1996-05-12
    20     4958389621322070       09/32                VISA 16 digit             2016-01-21
    21  4305758492757250000       09/28                VISA 19 digit             2014-01-01
    22     5386813678403950       08/31                   Mastercard             2003-06-05
    23     4195679942480940       04/28                VISA 16 digit             2014-06-16
    24  4957130953614380000       11/26                VISA 19 digit             1995-10-20
    25         584413868780       05/23                      Maestro             1998-02-12
    26       30066133851589       04/24  Diners Club / Carte Blanche             2005-03-06
    27     6011277092557040       05/31                     Discover             2014-08-21
    28  4847729374874600000       02/29                VISA 19 digit             2021-11-05
    29  4627956208650560000       03/24                VISA 19 digit             2021-03-01
    30     2604195282355750       03/29                   Mastercard             2007-03-05
    31     6011826162642800       02/28                     Discover             2015-10-07
    32  4405805374008450000       04/23                VISA 19 digit             1998-10-10
    33     2658148348730760       07/25                   Mastercard             2022-05-28
    
    ```
#### list_number_of_stores:
This function connect to the API and return the total number of available stores in the end point.
It takes as arguments:
 - number_of_stores_url: 
    The url end point to collect the total number of stores
 - header_keys: 
    API key authorisation, key:value pair as a dictionary
Return the total number of stores available in the given end point.
Below is the screenshot of the described function
    ```
    def list_number_of_stores(
            self,
            number_of_stores_url:str,
            header_keys:dict
            ):    
            
            '''
            This function connect to the API and return the total number of available stores in the end point
            Parameters:
            ------------
            number_of_stores_url: 
            The url end point to collect the total number of stores
            
            header_keys: 
            API key authorisation, key:value pair as a dictionary
            
            Return:
            Return the total number of stores available in the end point.
            
            '''
            
            response = requests.get(number_of_stores_url,headers=header_keys)
            if response.status_code ==200:
                stores_numbers= response.json()
                total_number_of_stores = stores_numbers
                print(f"The total number of stores is: {total_number_of_stores['number_stores']}")
            
            else:       
                print(f"response failed with status code: {response.status_code}")
                print(f"Response Text:{response.text}")
                
            
            return total_number_of_stores['number_stores']
    
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
    #print(tables_list.read_rds_table(table_name=users_table,user_access=user_credentials,database_type=database_type,database_api=dbapi))
    
    #pdf_data = tables_list.retrieve_pdf_data(data_url=card_data)
    #print(pdf_data)
    
    print(tables_list.list_number_of_stores(end_point,tokens))
    ##### Result after running the script###
    
    LOADING CREDENTIALS...
    CREDENTIALS SUCCESSFULLY LOAD
    LOADING CREDENTIALS...
    CREDENTIALS SUCCESSFULLY LOAD
    INITIATING DATABASE...
    DATABASE ENGINE SUCCESSFULLY INITIATED
    The total number of stores is: 451
    
    ```

#### retrieve_stores_data():
This function retrieves data store in the API, using x-api-key.
This function list the total number of stores and retrieves data for every store,
and store it into a CSV file localy. The function takes as arguments:
    - store_number_url: 
       Url link to read the total number of available stores.
    - store_url: 
      Url link to extract stores data
    - api_keys: 
      The headers  access keys.
    - cvs_file_path: 
      The file path to store data collected in a csv file.
      ```
          
    def retrieve_stores_data(
        self,
        store_number_url:str,
        store_url:str,
        api_keys:dict,
        csv_file_path:str
        ):
        
        '''
        This function list the total number of stores and retrieves data for every store,
        and store it into a CSV file localy
        
        Parameters
        -------------
        store_number_url: 
        Url link to read the total number of available stores.
        store_url: 
        Url link to extract stores data
        api_keys: 
        The headers  access keys.
        cvs_file_path: 
        The file path to store the  data in a csv file
        
        Return
        ----------
        Return a store data as a pandas dataframe
        '''
        
        total_number_of_stores = self.list_number_of_stores(store_number_url,api_keys)
        print(type(total_number_of_stores))
        list_stores_data = []
        for store_index in range(total_number_of_stores):
            store_number = store_index
            response = requests.get(store_url+str(store_number),headers=api_keys)
            
            if response.status_code == 200:
                store_details = response.json()
                if store_details:
                    print(f'STORE : {store_number} RETRIEVED SUCCESSFULLY.')
                    
                else:
                    print('DATA NOT RETRIEVED..')
                list_stores_data.append(store_details)
                
                print("DATA STORE SUCCESSFULLY")
    
            else:
                print(f"Response with store number {store_number} failed with status code:{response.status_code}")
                print(response.text)
                time.sleep(1)
        print("INITIATING DATAFRAME...")        
        stores_details_df = pd.DataFrame(list_stores_data)
        if True:
            print("DATAFRAME CREATED.")
            time.sleep(1)
        
        stores_details_df.to_csv(csv_file_path)
        if True:
            print(f"CSV FILE: {csv_file_path} SUCCESSFULLY CREATED")
            
        return csv_file_path

     After running the script:
     PS C:\Users\lesaint\Documents\Multinational_Retail_Data_Collection> python3 .\data_extraction.py
    LOADING CREDENTIALS...
    CREDENTIALS SUCCESSFULLY LOAD
    LOADING CREDENTIALS...
    CREDENTIALS SUCCESSFULLY LOAD
    INITIATING DATABASE...
    DATABASE ENGINE SUCCESSFULLY INITIATED
    The total number of stores is: 451
    <class 'int'>
    STORE : 0 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 1 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 2 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 3 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 4 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 5 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 6 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 446 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 447 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 448 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 449 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    STORE : 450 RETRIEVED SUCCESSFULLY.
    DATA STORE SUCCESSFULLY
    INITIATING DATAFRAME...
    DATAFRAME CREATED.
    CSV FILE: stores_data.csv SUCCESSFULLY CREATED
    stores_data.csv
    
          ```
  In this function we called the list_number_of_stores() function to retrieve stores, and used alist to srore all stores
  then use the request library to link each store number with it data and save it in JSON format.After the opration we 
  we convert the stores in a unique dataframe and save it in a csvv file localy.

#### extract_from_s3()
This function extract a csv file  from AWS s3 bucket,it akes as arguments:
    - csv_s3_address:
       AWS URL address of the csv s3 bucket.
The function return a file in a Dataframe format as illustrated below

  ```
     def extract_from_s3(self,csv_s3_address:str):
          '''
          This function extract a csv file  from AWS s3 bucket
          Parameters:
          -----------------
          csv_s3_address: The address of the csv s3 bucket
          
          Return:
          --------
          return a file in a Dataframe format
          '''
          s3_df_products = pd.read_csv(csv_s3_address,index_col=0)
          
          return s3_df_products
       After running the script
      PS C:\Users\lesaint\Documents\Multinational_Retail_Data_Collection> python3 .\data_extraction.py
  LOADING CREDENTIALS...
  CREDENTIALS SUCCESSFULLY LOAD
  LOADING CREDENTIALS...
  CREDENTIALS SUCCESSFULLY LOAD
  INITIATING DATABASE...
  DATABASE ENGINE SUCCESSFULLY INITIATED
                                         product_name product_price  weight        category  ...  date_added                                  uuid          removed product_code
    0     FurReal Dazzlin' Dimples My Playful Dolphin        £39.99   1.6kg  toys-and-games  ...  2005-12-02  83dc0a69-f96f-4c34-bcb7-928acae19a94  Still_avaliable  R7-3126933h
    1             Tiffany's World Day Out At The Park        £12.99  0.48kg  toys-and-games  ...  2006-01-09  712254d7-aea7-4310-aff8-8bcdd0aec7ff  Still_avaliable  C2-7287916l
    2             Tiffany's World Pups Picnic Playset         £7.00    590g  toys-and-games  ...  1997-03-29  b089ef6f-b628-4e37-811d-fffe0102ba64  Still_avaliable  S7-1175877v
    3        Tiffany's World Wildlife Park Adventures        £12.99    540g  toys-and-games  ...  2013-03-20  d55de422-8b98-47d6-9991-e4bc4c5c0cb0          Removed  D8-8421505n
    4                         Cosatto Cosy Dolls Pram        £30.00  1.91kg  toys-and-games  ...  2007-12-23  7945b657-cb02-4cc5-96cf-f65ed0a8f235  Still_avaliable  B6-2596063a
     
```

#### extract_json_from_():

This function extract JSON data store in AWS url.
The function takes as argument:
 - json_s3_address:
   The AWS s3_bucket url address where the data is stored.
Below is the screenshot of the function
   ```
   
    def extract_json_from_s3(self,json_s3_addresss3_address:str):
        '''
        This function extract a json data file from AWS s3 bucket
        Parameters:
        -----------------
        json_s3_address: The address of the json s3 bucket
        
        Return:
        -------
        return a pandas data Dataframe
        '''
        s3_df_product = pd.read_json(json_s3_addresss3_address)
        
        return s3_df_product
         
   ```
   ```
  
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
      
      print(tables_list.extract_json_from_s3(s3_dates))
       After running the script:
          LOADING CREDENTIALS...
      CREDENTIALS SUCCESSFULLY LOAD
      LOADING CREDENTIALS...
      CREDENTIALS SUCCESSFULLY LOAD
      INITIATING DATABASE...
      DATABASE ENGINE SUCCESSFULLY INITIATED
             timestamp month  year day time_period                             date_uuid
      0       22:00:06     9  2012  19     Evening  3b7ca996-37f9-433f-b6d0-ce8391b615ad
      1       22:44:06     2  1997  10     Evening  adc86836-6c35-49ca-bb0d-65b6507a00fa
      2       10:05:37     4  1994  15     Morning  5ff791bf-d8e0-4f86-8ceb-c7b60bef9b31
      3       17:29:27    11  2001   6      Midday  1b01fcef-5ab9-404c-b0d4-1e75a0bd19d8
      4       22:40:33    12  2015  31     Evening  dfa907c1-f6c5-40f0-aa0d-40ed77ac5a44
      ...          ...   ...   ...  ..         ...                                   ...
      120156  22:56:56    11  2022  12     Evening  d6c4fb31-720d-4e94-aa6b-dcbcb85f2bb7

   ```
  ### 4.c Data_cleaning.py:
  The Data cleaning python file is used to clean the data after being retrieved.
  Most data after extraction contains different data types,and sometimes mixed with bad characters,
  within this file we defined many functions to clean data coming from different data sources
  before uploading them to the databaseaccurate.
  The screenshot below illustrate the cleaning process of data.
     ```
     
                 def clean_user_data(
                    self,
                    db_table_name:str,
                    access_tokens:str,
                    database_type:str,
                    database_api:str,
                    host:str,
                    database_user:str,
                    password:str,
                    database_name:str,
                    port:int,
                    table_name:str
                    ):
                    
                    '''
                    This function clean users data using pandas
                    '''
                    user_data = DataExtractor().read_rds_table(
                        db_table_name,
                        access_tokens,
                        database_type,
                        database_api
                        )
                    
                    user_data.set_index('index',inplace=True)
                    
                    print('USERS DATA INFO ')
                    print(user_data.info())
                    print(f"Checking NaN values\n: {user_data.isna().sum()}")
                    user_data.dropna(inplace=True)
                    time.sleep(1)
                    print("CLEANING DIM_USERS_TABLES")
                    user_data['first_name']=user_data['first_name'].astype('string')
                    user_data['first_name']=user_data['first_name'].apply(lambda x: ''.join([char if ord(char) < 
            128 else '' for char in x])).astype('string')
                    user_data['first_name']=user_data['first_name'].apply(lambda x: re.sub(r'\W+', '', x)).astype('string')
                    user_data['first_name'] = user_data['first_name'].astype('string').str.replace('\d+','')
                    
                    user_data['last_name']=user_data['last_name'].astype('string')
                    user_data['last_name']=user_data['last_name'].apply(lambda x: ''.join([char if ord(char) < 128 else '' for char in x])).astype('string')
                    user_data['last_name']=user_data['last_name'].apply(lambda x: re.sub(r'\W+', '', x)).astype('string')
                    user_data['last_name'] = user_data['last_name'].astype('string').str.replace('\d+','')
                    
                    user_data['company'] = user_data['company'].astype('string')
                    user_data['company'] = user_data['company'].apply(lambda x: re.sub(r'\W', ' ', x).title()).astype('string')
                    
                    uniques_countries = ['Germany', 'United Kingdom', 'United States']
                    user_data = user_data[user_data['country'].isin(uniques_countries)]
                    user_data['country'] = user_data['country'].astype('string')
                    
                    
                    user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'],format='mixed')
                    #user_data['date_of_birth']=user_data['date_of_birth'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
                    print(f"Checking NaN values in Date of Birth Column: {user_data['date_of_birth'].isna().sum()}")
                    
                    
                    user_data['email_address'] = user_data['email_address'].astype('string')
                    def check_email_format(emails):
                        
                        if "@" in emails:   
                            return emails
                               
                    user_data['email_address'] = user_data['email_address'].apply(check_email_format).astype('string')
                    
                    user_data['address'] = user_data['address'].astype('string')
                    user_data['address'] = user_data['address'].apply(lambda x: re.sub(r'[^\w\s]', '', x).title()).astype('string')
                    def remove_special_characters(s): 
                        return re.sub(r'[^a-zA-Z0-9\s]', '', str(s)) 
                    #Apply the function to the column 
                    user_data['address']=user_data['address'].apply(remove_special_characters).astype('string')
                    user_data['address'] = user_data['address'].apply(lambda x: " ".join(x.split())).astype('string')
                    
                    
                    user_data['country_code'] = user_data['country_code'].astype('string')
                    user_data['country_code'] = user_data['country_code'].str.replace('GGB','GB')
                    #unique_codes =['DE','GB','US']
                    #user_data['country_code']=user_data['country_code'].apply(lambda x: x if x in unique_codes else np.nan)
                    user_data['country_code'] = user_data['country_code'].astype('string')
                    
                    user_data['phone_number'] = user_data['phone_number'].apply(lambda x: ''.join([number for number in str(x) if number.isnumeric()])).astype('int64')
                    
                    user_data['join_date']= pd.to_datetime(user_data['join_date'],format='mixed')
                    #user_data['join_date']=user_data['join_date'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
                    
                    print(f"Checking NaN values after cleaning:\n{user_data.isna().sum()}")
                    user_data.reset_index(inplace=True)
                    print(user_data.info())
            
                    print(f"UPLOADING CLEANED DATA TO THE DATABASE TABLE {table_name} OF {database_name} DATABASE..")
                    
                    try:
                        print("\tESTABLISHING DATABASE CONNECTION...")
                        connection = DatabaseConnector()
                   
                        if True:          
                            upload_data =connection.upload_to_db(
                                host=host,
                                user=database_user,
                                password=password,
                                database_name=database_name,
                                port=port,
                                user_data=user_data,
                                table_name=table_name
                                )
                            
                            print(f"\tTABLE {table_name} OF DATABASE: {database_name} UPDATED")
                                           
                            return f"USERS DATA TABLE: \n{user_data}"
                    except Exception as error:
                        print("CONNECTION TO THE DATABASE FAILED")
                            
            
   ```
The screenshot above shows how we define a function clean_user_data(), to clean data retrieved from
a database table, and upload it into the user database table.
the screenshot below illustrate the cleaning process of a tabular data retrieved from pdf url link.
   ```
             def clean_card_data(
                self,
                data_link:str,
                host:str,
                database_user:str,
                password:str,
                database_name:str,
                port:int,
                table_name:str
                ):
                
                data = DataExtractor().retrieve_pdf_data(data_link)
                
                print(f'Found {len(data)} tables')
                cards_data = pd.concat(data,ignore_index=True)
                
                cards_data['expiry_date']=pd.to_datetime(cards_data['expiry_date'],format='%m/%y',errors='coerce')
                cards_data['expiry_date']=cards_data['expiry_date'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
                
                cards_data['card_provider'] = cards_data['card_provider'].astype('string')
                providers = ['Diners Club / Carte Blanche','American Express','JCB 16 digit','JCB 15 digit','Maestro','Mastercard','Discover','VISA 19 digit','VISA 16 digit','VISA 13 digit']
                cards_data = cards_data[cards_data['card_provider'].isin(providers)]
                print(f"\tList of uniques values in card provider column :{cards_data['card_provider'].unique()}")
                
                cards_data['card_number']=cards_data['card_number'].apply(lambda x: ''.join([number for number in str(x) if number.isnumeric()]))
                cards_data['card_number'] = cards_data['card_number'].str.replace(r'\D+', '')
                cards_data['card_number']=cards_data['card_number'].str.replace(r'[^0-9]+', '')
                cards_data['card_number'] = cards_data['card_number'].astype('int64')
        
                card_type_validation = is_numeric_dtype(cards_data['card_number'])
                if card_type_validation:
                    print('\tCards numbers are numerics')
                else:
                    print('Errors')
                
                cards_data['date_payment_confirmed']=pd.to_datetime(cards_data['date_payment_confirmed'],errors='coerce',infer_datetime_format=True,format='mixed')
                print(cards_data.info())
                
                print(f"UPLOADING DATA TO DATABASE TABLE {table_name} OF {database_name} DATABASE..")
                
                try:
                    print("\tESTABLISHING DATABASE CONNECTION...")
                    connection = DatabaseConnector()
               
                    if True:          
                        upload_data =connection.upload_to_db(
                            host=host,
                            user=database_user,
                            password=password,
                            database_name=database_name,
                            port=port,
                            user_data=cards_data,
                            table_name=table_name
                            )
                        
                        print(f"\tTABLE {table_name} OF DATABASE: {database_name} UPDATED")
                                       
                        return f"CARDS DATA TABLE: \n{cards_data}"
                except Exception as error:
                    print("CONNECTION TO THE DATABASE FAILED")
                    
   ```
The example below shows of the cleaning process of data retrived from an url link,then upload to the database.

     ```
            def called_clean_store_data(
                self,
                stores_number_url:str,
                store_link:str,
                access_keys:dict,
                csv_file,host:str,
                database_user:str,
                password:str,
                database_name:str,
                port:int,
                table_name:str
                ):
                
                data = DataExtractor()
                stores = data.retrieve_stores_data(
                    stores_number_url,
                    store_link,
                    access_keys,
                    csv_file
                    )
                print("\tCLEANING STORES DATA...")
                pd.set_option('display.max_columns', None)
                store_df = pd.read_csv(stores,index_col=0)
                print("SETTING COLUMN INDEX")
                store_df.set_index('index',inplace=True)
                print(store_df.info())
 store_df['opening_date']=pd.to_datetime(store_df['opening_date'],errors='coerce',infer_datetime_format=True,format='mixed')
                print('Setting Countries codes')
                store_df['country_code'].unique()
                store_code = ['GB', 'DE', 'US']
                store_df = store_df[store_df['country_code'].isin(store_code)]
                store_df['country_code']=store_df['country_code'].astype('string')
                
                print("Setting up proper continents names")
                store_df['continent'] = store_df['continent'].astype('string')
                store_df['continent'] = store_df['continent'].replace('eeEurope','Europe' )
                store_df['continent'] = store_df['continent'].replace('eeAmerica','America')
                print(store_df['continent'].unique())
                
                print(f"\tList of stores type: {store_df['store_type'].unique()}")
                store_df['store_type'] = store_df['store_type'].astype('string')
                
                store_df['staff_numbers']=store_df['staff_numbers'].apply(lambda x: ''.join([number for number in str(x) if number.isnumeric()]))
                store_df['staff_numbers']=store_df['staff_numbers'].str.replace(r'\D+', '')
                store_df['staff_numbers'] = store_df['staff_numbers'].astype(int)
                staff_numbers_validation = is_numeric_dtype(store_df['staff_numbers'])
                if staff_numbers_validation:            
                    print('The staff_numbers column contains only numerical values')
                else:        
                    print('check data format')
        
                
                store_df['store_code']=store_df['store_code'].apply(lambda x: str(x).replace('\W','')).astype('string')
                number_of_store = store_df['store_code'].count()
                store_code_format = store_df['store_code'].str.upper().count()
                if store_code_format == number_of_store:        
                    print('All data in store_code column are formatted in Uppercase format. ')
                else:
                    print('please check the data')
                 
                    
                store_df['locality'] = store_df['locality'].astype('string').str.replace('\d+','')
                store_df['locality']=store_df['locality'].str.replace(r'[^A-Za-z]+', '')
                
                store_df['lat']=store_df['lat'].astype('string')
                
                store_df['longitude']=store_df['longitude'].str.replace(r'[^0-9\-\.\,]+', '').astype('string')
                
                store_df['latitude']=store_df['latitude'].str.replace(r'[^0-9\-\.\,]+', '').astype('string')
                
                def remove_special_characters(s):    
                    return re.sub(r'[^a-zA-Z0-9\s]', '', str(s)) 
                store_df['address']=store_df['address'].apply(remove_special_characters).astype('string')
                store_df['address'] = store_df['address'].apply(lambda x: " ".join(x.split())).astype('string')
                print(store_df.info())
                
                print(f"UPLOADING DATA TO DATABASE TABLE {table_name} OF {database_name} DATABASE..")
        
                try:
                    print("\tESTABLISHING DATABASE CONNECTION...")
                    
                    connection = DatabaseConnector()
                    if True:          
                        upload_data =connection.upload_to_db(
                            host=host,
                            user=database_user,
                            password=password,
                            database_name=database_name,
                            port=port,
                            user_data=store_df,
                            table_name=table_name
                            )
                        
                        print(f"\tTABLE {table_name} OF DATABASE: {database_name} UPDATED")
                                       
                        return f"STORES DATA TABLE: \n{store_df}"
                except Exception as error:
                    print("CONNECTION TO THE DATABASE FAILED")                
   ```
### 4.d Data_processing.py:
The data processing python file is the main file for this project, whithin it we define a fonction to process the application.
Below is the screenshot of the Data_processing.py file
```
  import time
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


def loads_transform_data():
    cleaning = DataCleaning()   
    
    #Database acess credentials
    
    user_credentials='db_creds.yaml' 
    database_type='postgresql'
    dbapi='psycopg2'
    host='localhost'
    user= 'postgres'
    password='-----------'
    database = 'sales_data'
    port= 5432
    
    # Database tables names to upload data
    dim_user_table = 'dim_users'
    card_table = 'dim_card_details'
    store_table = 'dim_store_details'
    data_order_table ='orders_table'
    product_table = 'dim_products' 
    dates_table = 'dim_date_times' 
        
    # Data sources to retrieve data
    users_table = 'legacy_users'
    order_table = 'orders_table'
    card_data = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'  
    end_point2 = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    tokens = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    s3_products = 's3://data-handling-public/products.csv'
    s3_dates = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'   
    
    # Data storage files
    store_csv_file = 'stores_data.csv'

      
        
    print("CLEANING USERS DATA...")
    user_cleaning = cleaning.clean_user_data(
        users_table,
        user_credentials,
        database_type,
        dbapi,
        host,
        user,
        password,
        database,
        port,
        dim_user_table
        )
    print(user_cleaning)
    print("CLEANIND COMPLETED")
    time.sleep(2)
    
    print("CLEANING CARDS DATA...")
    cards_cleaning = cleaning.clean_card_data(
        card_data,
        host,
        user,
        password,
        database,
        port,
        card_table
        )
    print(cards_cleaning)
    print("CLEANING COMPLETED")
    time.sleep(2)
    
    print("CLEANING STORES DATA...")  
    stores_cleaning = cleaning.called_clean_store_data(
        end_point,
        end_point2,
        tokens,
        store_csv_file,
        host=host,
        database_user=user,
        password=password,
        database_name=database,
        port=port,
        table_name=store_table
        )
    print(stores_cleaning)
    print("CLEANING COMPLETED") 
    time.sleep(2)
    
    print("CLEANING ORDERS DATA...")
    orders_cleaning = cleaning.clean_orders_data(
        order_table,
        user_credentials,
        database_type,
        dbapi,
        host,
        user,
        password,
        database,
        port,
        data_order_table
        )
    print(orders_cleaning)
    print("CLEANING COMPLETED")
    time.sleep(2)
    
    
    print("CLEANING PRODUCTS DATA...")
    s3_products_data = cleaning.clean_products_data(
        s3_products,
        host,
        user,
        password,
        database,
        port,
        product_table
        )
    print(s3_products_data)
    print("CLEANING COMPLETED")
    time.sleep(2)
       
    #print("CLEANING DATES EVENTS DATA...")  
    dates_events_cleaning = cleaning.clean_dates_events(
        s3_dates,
        host,
        user,
        password,
        database,
        port,
        dates_table
        )
    print(dates_events_cleaning)
    print("CLEANING COMPLETED")
      


if __name__== '__main__':
    print("STARTING DATA EXTRACT AND CLEANING...")
    loads_transform_data()
    print("\tDATABASE UPDATED WITH SUCCESS")

```

## Querying_Data
   After retrieving and storing the data to the database server, we performed Sql queries operations to clean the data and 
   update data with specific data types,
   also create a relational database model with the tables by joining them together with foreign keys constraints.
   The screenshot below illustrate the mapping of the tables.
   
```
           /* UPDATING MULTINATIONAL DATABASE DATA*/
    
        /* UPDATING ORDERS_TABLE*/
        
        select * from orders_table
        
        CREATE EXTENSION "uuid-ossp";
        
        ALTER TABLE orders_table
        
        	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
    	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    	ALTER COLUMN store_code TYPE VARCHAR(25),
    	ALTER COLUMN product_code TYPE VARCHAR(25),
    	ALTER COLUMN product_quantity TYPE SMALLINT,
    	ALTER COLUMN card_number TYPE VARCHAR(19);
    
    /* UPDATING DIM_USERS TABLE*/
    
    SELECT * FROM dim_users
    
    ALTER TABLE dim_users
        ALTER COLUMN first_name TYPE VARCHAR(255),
    	ALTER COLUMN last_name TYPE VARCHAR(255),
    	ALTER COLUMN date_of_birth TYPE DATE,
    	ALTER COLUMN country_code TYPE VARCHAR(2),
    	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
    	ALTER COLUMN join_date TYPE DATE;
    
    
    /* UPDATING DIM_STORE DATA */
    
    SELECT * FROM dim_store_details
    
    ALTER TABLE dim_store_details
        ALTER COLUMN longitude TYPE FLOAT,
    	ALTER COLUMN locality TYPE VARCHAR(255),
    	ALTER COLUMN store_code TYPE VARCHAR(25),
    	ALTER COLUMN staff_numbers TYPE SMALLINT,
    	ALTER COLUMN opening_date TYPE DATE,
    	ALTER COLUMN store_type TYPE VARCHAR(255),
    	ALTER COLUMN latitude TYPE FLOAT,
    	ALTER COLUMN country_code TYPE VARCHAR(2),
    	ALTER COLUMN continent TYPE VARCHAR(255);
    
    
    /* UPDATING DIM_PRODUCTS DATA*/
    
    SELECT * FROM dim_products
    
    ALTER TABLE dim_products
        ADD weight_class VARCHAR(255);
    	
    UPDATE dim_products
    SET weight_class = (CASE
        WHEN weight < 2 THEN 'Light'
    	WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    	WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    	ELSE 'Truck_Required'
    END);
    
    ALTER TABLE dim_products
        RENAME COLUMN removed TO still_available;
    
    ALTER TABLE dim_products
        ALTER COLUMN product_price TYPE FLOAT,
    	ALTER COLUMN weight TYPE FLOAT,
    	ALTER COLUMN "EAN" TYPE VARCHAR(255),
    	ALTER COLUMN product_CODE TYPE VARCHAR(25),
    	ALTER COLUMN date_added TYPE DATE,
    	ALTER COLUMN  uuid TYPE UUID USING (uuid_generate_v4()),
    	ALTER COLUMN still_available TYPE boolean
    	USING CASE still_available WHEN 'Still_available' THEN true ELSE false END,
    	ALTER COLUMN weight_class TYPE VARCHAR(14);
    	
    	
    
    /* UPDATING DIM_DATE_TIMES DATA */
    
    SELECT * FROM dim_products;

    ALTER TABLE dim_date_times
        ALTER COLUMN month TYPE VARCHAR(2),
    	ALTER COLUMN year TYPE VARCHAR(4),
    	ALTER COLUMN day TYPE VARCHAR(2),
    	ALTER COLUMN time_period TYPE VARCHAR(12),
    	ALTER COLUMN  date_uuid TYPE UUID USING (uuid_generate_v4());
    
    
    /* UPDATING DIM_CARD_DETAILS*/
    
    SELECT * FROM dim_card_details
    
    ALTER TABLE dim_card_details
        ALTER COLUMN card_number TYPE VARCHAR(19),
    	ALTER COLUMN date_payment_confirmed TYPE DATE,
    	ALTER COLUMN expiry_date TYPE VARCHAR(7);
    UPDATE dim_card_details 
         SET expiry_date = substring(expiry_date, 1,7);
    
    /* UPDATING TABLES WITH PRIMARY KEYS*/
    ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
    ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
    ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
    ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
    ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);
    
        /* UPDATING FOREIGN KEYS*/
        SELECT * FROM orders_table
        
        ALTER TABLE orders_table
            ADD CONSTRAINT fk_card_orders_table FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);
        
        ALTER TABLE orders_table
            ADD CONSTRAINT fk_dates_times_table FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);
        	
        ALTER TABLE orders_table
            ADD CONSTRAINT fk_products_table FOREIGN KEY (product_code) REFERENCES dim_products (product_code);
        	
        ALTER TABLE orders_table
            ADD CONSTRAINT fk_stores_table FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);
        	
        ALTER TABLE orders_table
            ADD CONSTRAINT fk_users_table FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);

```   
After mapping the database, we query the database to get an update metrics of data.
Below are  screenshots illustrating the data manipulation.

- Querying numbers of stores in each country:
 
![2024-04-02](https://github.com/juclesaint17/Multinational-Retail-Data-Centralisation/assets/94936087/9f8bdc85-d20b-4e73-90a8-81e112abe924)

-Querying locality with higher number of stores:

![2024-04-02 (1)](https://github.com/juclesaint17/Multinational-Retail-Data-Centralisation/assets/94936087/9ed8f767-a218-4737-afbf-35d9658f24e2)

-Querying monthly largest sales:


![2024-04-02 (2)](https://github.com/juclesaint17/Multinational-Retail-Data-Centralisation/assets/94936087/34453cf5-0170-4e97-a042-b9e2788d148f)

-Querying online and offline products quantity count:

![2024-04-02 (3)](https://github.com/juclesaint17/Multinational-Retail-Data-Centralisation/assets/94936087/5656ddb0-614d-4f27-8883-46aabcd6a98e)

-Querying percentage of sales by store:

![2024-04-02 (4)](https://github.com/juclesaint17/Multinational-Retail-Data-Centralisation/assets/94936087/080e4b8c-e104-4836-b381-81329d66989d)

-Querying higher monthly cost by year:
![2024-04-02 (5)](https://github.com/juclesaint17/Multinational-Retail-Data-Centralisation/assets/94936087/624f8b91-5a11-4540-8419-c0faba271c5b)

-Querying overall staff headcount by country:

![2024-04-02 (6)](https://github.com/juclesaint17/Multinational-Retail-Data-Centralisation/assets/94936087/52f9b1ec-3de4-4b84-8fb6-336ff759a7e6)

-Querying Germain store top selling:

![2024-04-02 (7)](https://github.com/juclesaint17/Multinational-Retail-Data-Centralisation/assets/94936087/26d59e20-22ad-4db7-84c2-a4d5333db11b)

-Querying year sales average time:

![2024-04-02 (8)](https://github.com/juclesaint17/Multinational-Retail-Data-Centralisation/assets/94936087/8f6929b0-40e0-4b91-8668-7d0ed976a64a)
