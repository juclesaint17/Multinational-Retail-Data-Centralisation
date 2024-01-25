from decimal import Decimal
from pandas.api.types import is_numeric_dtype,is_string_dtype
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import datetime
import pandas as pd
import tabula
import re
import json
import numpy as np
import boto3
import s3fs
import time



class DataCleaning:
    
    def __init__(self):
        
        self.s3_products = ''
        self.order_data =''
        
    
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
        This function clean user data using pandas
        '''
        user_data = DataExtractor().read_rds_table(
            db_table_name,
            access_tokens,
            database_type,
            database_api
            )
        copy_data3 = user_data.copy()
        copy_data3.set_index('index',inplace=True)
        print(copy_data3.info())
        time.sleep(1)
        print("\tCLEANING FIRST NAME COLUMN....")
        copy_data3['first_name']=copy_data3['first_name'].astype('string')
        print(f"Checking total NaN values in First Name column: {copy_data3['first_name'].isna().sum()}")
        copy_data3.dropna(subset=['first_name'], inplace=True)
        copy_data3['first_name']=copy_data3['first_name'].apply(lambda x: ''.join([char if ord(char) < 128 else '' for char in x])).astype('string')
        copy_data3['first_name']=copy_data3['first_name'].apply(lambda x: re.sub(r'\W+', '', x)).astype('string')
        copy_data3['first_name'] = copy_data3['first_name'].astype('string').str.replace('\d+','')
        print(f"Checking total NaN values in First Name column after cleaning : {copy_data3['first_name'].isna().sum()}")
        
        print("\tCLEANING LAST NAME COLUMN.")
        copy_data3['last_name']=copy_data3['last_name'].astype('string')
        print(f"Checking total NaN values in Last Name column: {copy_data3['last_name'].isna().sum()}")
        copy_data3.dropna(subset=['last_name'], inplace=True)
        print(f"Checking total NaN values in Last Name column: {copy_data3['last_name'].isna().sum()}")
        copy_data3['last_name']=copy_data3['last_name'].apply(lambda x: ''.join([char if ord(char) < 128 else '' for char in x])).astype('string')
        copy_data3['last_name']=copy_data3['last_name'].apply(lambda x: re.sub(r'\W+', '', x)).astype('string')
        copy_data3['last_name'] = copy_data3['last_name'].astype('string').str.replace('\d+','')
        
        print('\tCLEANING DATE OF BIRTH COLUMN')
        copy_data3['date_of_birth'] = pd.to_datetime(copy_data3['date_of_birth'],errors='coerce',infer_datetime_format=True,format='mixed')
        print(f"Checking NaN values in Date of Birth Column: {copy_data3['date_of_birth'].isna().sum()}")
        copy_data3['date_of_birth']=copy_data3['date_of_birth'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
        copy_data3.dropna(subset=['date_of_birth'], inplace=True)
        print(f"Checking NaN values in Date of Birth Column after droping: {copy_data3['date_of_birth'].isna().sum()}")
        
        print('\tCLEANING COMPANY COLUMN')
        print(f"Checking total NaN values in Company column: {copy_data3['company'].isna().sum()}")
        copy_data3['company'] = copy_data3['company'].astype('string')
        copy_data3['company']=copy_data3['company'].apply(lambda x: re.sub(r'\W+', ' ', x).title()).astype('string')
        copy_data3['company']=copy_data3['company'].apply(lambda x: x if isinstance(x, str)else np.nan).astype('string')
        print(f"Checking total NaN values in Company column after droping: {copy_data3['company'].isna().sum()}")
        
        print("\tCLEANING EMAIL ADDRESS COLUMN")
        print(f"Checking unique email address in column: {copy_data3['email_address'].is_unique}")
        copy_data3['email_address'] = copy_data3['email_address'].astype('string')

        def check_email_format(emails):
            
            if "@" in emails:   
                return emails
            else:
                np.nan    
        copy_data3['email_address'] = copy_data3['email_address'].apply(check_email_format).astype('string')
        print(f"Checking NaN address in column: {copy_data3['email_address'].isna().sum()}")
        
        print('\tCLEANING ADDRESS COLUMN')
        copy_data3['address'] = copy_data3['address'].apply(lambda x: re.sub(r'[^\w\s]', '', x).title()).astype('string')
        print(f"Checking NaN values in column: {copy_data3['address'].isna().sum()}")
        
        print('\t CLEANING COUNTRY CODE COLUMN')
        copy_data3['country_code'].unique()
        print(copy_data3['country_code'].unique())
        copy_data3['country_code'] = copy_data3['country_code'].astype('string').str.replace('GGB','GB')
        
        print('\tCLEANING PHONE NUMBER COLUMN')
        copy_data3['phone_number'] = copy_data3['phone_number'].apply(lambda x: ''.join([number for number in str(x) if number.isnumeric()])).astype('int64')
        print(f"checking NaN values in column: {copy_data3['phone_number'].isna().sum( )}")
        print(f"checking if phones numbers are unique in column: {copy_data3['phone_number'].is_unique}")
        copy_data3.drop_duplicates(subset=['phone_number'],inplace=True)
        print(f"checking if phones numbers are unique in column after droping duplicated : {copy_data3['phone_number'].is_unique}")
        print(f"checking if phones numbers have NaN values in column  : {copy_data3['phone_number'].isna().sum()}")
        
        copy_data3['join_date']= pd.to_datetime(copy_data3['join_date'],errors='coerce',infer_datetime_format=True,format='mixed')
        print(f"checking if joined date column have NaN values: {copy_data3['phone_number'].isna().sum()}")
        copy_data3['join_date']=copy_data3['join_date'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
        print(f"checking if joined date column have NaN values after cleaning: {copy_data3['phone_number'].isna().sum()}")
        
        print(f"checking if user_uuid are unique in column: {copy_data3['user_uuid'].is_unique}")
        print(f"checking if user_uuid column have NaN values: {copy_data3['phone_number'].isna().sum()}")
        copy_data3.reset_index(inplace=True)
        
        connection =  DatabaseConnector()
        upload_data = connection.upload_to_db(
            host=host,
            user=database_user,
            password=password,
            database_name=database_name,
            port=port,
            user_data=copy_data3,
            table_name=table_name
            )
        if True:
            print(f'TABLE {table_name} SUCCESSFULLY UPLOADED TO DATABASE {database_name}')
            
        print(copy_data3.info())
              
        return copy_data3

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
        
        card_data = DataExtractor().retrieve_pdf_data(data_link)
        print(f'Found {len(card_data)} tables')
        cards_data = pd.concat(card_data,ignore_index=True)
        print(cards_data)
        
        print("...Cleaning data...")
        cards_data['card_number']=cards_data['card_number'].apply(lambda x: x if len(str(x)) >=12 and len(str(x))<=19 else np.nan) #1
        cards_data['card_number']=pd.to_numeric(cards_data['card_number'],errors='coerce').fillna(0).astype('int64') #2
        cards_data['card_number'].isna().sum()
        
        print('Checking if all cards numbers are of type integer')
        
        card_type_validation = is_numeric_dtype(cards_data['card_number'])
        if card_type_validation:
            print('Cards numbers in the column are numerics')
        else:
            print('Errors')
        
        cards_data['expiry_date']=pd.to_datetime(cards_data['expiry_date'],format='%m/%y',errors='coerce')
        cards_data['expiry_date']=cards_data['expiry_date'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
        print('Checking NaN values in expiry date column..')
        print(' ')
        print('The total number of NaN values in expiry date column is:', cards_data['expiry_date'].isna().sum())
        cards_data.dropna(subset=['expiry_date'],inplace=True)
        print(f"The total number of NaN values after droping in expiry date is : {cards_data['expiry_date'].isna().sum()}")
        
        cards_data['date_payment_confirmed'] = pd.to_datetime(cards_data['date_payment_confirmed'],infer_datetime_format=True,errors='coerce',format='mixed')
        cards_data['date_payment_confirmed']=cards_data['date_payment_confirmed'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
        print(f"\nTotal number of NaN values in the date payment confirm column is: {cards_data['date_payment_confirmed'].isna().sum()}")
        
        print(f"List of uniques values in card provider column :{cards_data['card_provider'].unique()}")
        cards_data['card_provider'] = cards_data['card_provider'].astype('string')
        providers = ['Diners Club / Carte Blanche','American Express','JCB 16 digit','JCB 15 digit','Maestro','Mastercard','Discover','VISA 19 digit','VISA 16 digit','VISA 13 digit']
        cards_data = cards_data[cards_data['card_provider'].isin(providers)]
        
        print(f"\nTotal number of zeros values in card number column after replacing  np.nan=0 is: {cards_data.card_number[cards_data.card_number==0].count()}")
        cards_data=cards_data.loc[(cards_data[['card_number']] != 0).all(axis=1)]
        print(f"\nTotal number of zeros values in card number column after droping is: {cards_data.card_number[cards_data.card_number==0].count()}")
        print(cards_data.info())
        
        connection =  DatabaseConnector()
        upload_data =connection.upload_to_db(
            host=host,
            user=database_user,
            password=password,
            database_name=database_name,
            port=port,
            user_data=cards_data,
            table_name=table_name
         )
        
        if True:
            print(f'TABLE {table_name} SUCCESSFULLY UPLOADED TO DATABASE {database_name}')
        return cards_data
    
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
        print("CLEANING STORES DATA...")
        pd.set_option('display.max_columns', None)
        store_df = pd.read_csv(stores,index_col=0)
        print("SETTING COLUMN INDEX")
        store_df.set_index('index',inplace=True)
        print(store_df.info())
        print('CLEANING ADDRESS COLUMN')
        print(f"Total number of NaN values in address column is:{store_df['address'].isna().sum()}")
        store_df.dropna(subset=['address'], inplace=True)
        print(f"Total number of NaN values after cleaning the address column is: {store_df['address'].isna().sum()}")
        def remove_special_characters(s):
            
            return re.sub(r'[^a-zA-Z0-9\s]', '', s) 
        #Apply the function to the column 
        store_df['address'] = store_df['address'].apply(remove_special_characters).astype('string')
        
        print("CLEANING LONGITUDE COLUMN")
        store_df = store_df[pd.to_numeric(store_df['longitude'],errors='coerce').notnull()]
        store_df['longitude'] = store_df['longitude'].astype('float')
        longitude_nan = store_df['longitude'].isna().sum()
        print(f'total number of Nan values in longitude column is:{longitude_nan}')
        
        print('DROPING LAT COLUMN ')
        store_df = store_df.drop('lat',axis=1)
        
        print('CLEANING LOCALITY COLUMN')
        store_df['locality'] = store_df['locality'].astype('string')
        print(f"Total number of NaN values in locality column:{store_df['locality'].isna().sum()}")
        store_df['locality'] = store_df['locality'].str.strip() 
        store_df['locality']=store_df['locality'].map(lambda x: re.sub(r'\W+', ' ', x)).astype('string')
        store_df['locality'] = store_df['locality'].astype('string').str.replace('\d+','')
        def format_locality(data):
            
            if isinstance(data, str):                
                return data.title()
            else:
                np.nan
            return data
        print(f"Total number of NaN values in locality column after cleaning is:{store_df['locality'].isna().sum()}")
        
        print('CLEANING STORE CODE COLUMN')  
        print(f"Total number of NaN in store code column : {store_df['store_code'].isna().sum()}")
        number_of_store = store_df['store_code'].count()
        store_code_format = store_df['store_code'].str.upper().count()
        if store_code_format == number_of_store:        
            print('All data in store_code column are formatted in Uppercase format. ')
        else:
            print('Check data ')
            
        store_df['store_code'] = store_df['store_code'].apply(lambda x: str(x).upper()).astype('string')
        store_df['store_code'] = store_df['store_code'].apply(lambda x: str(x).replace('\W','')).astype('string')
        print(f"Checking total number of null values after cleaning: {store_df['store_code'].isna().sum()}")
        
        print('CLEANING STAFF NUMBERS COLUMN')
        store_df = store_df[pd.to_numeric(store_df['staff_numbers'],errors='coerce').notnull()]
        store_df['staff_numbers'] = store_df['staff_numbers'].astype('int64')
        print('') 
        staff_numbers_validation = is_numeric_dtype(store_df['staff_numbers'])
        if staff_numbers_validation:            
            print('The staff_numbers column contains only numerical values')
        else:        
            print('check data format')
        print(f"Total number of NaN values in staff_number column is: {store_df['staff_numbers'].isna().sum()}")
    
        
        
        print('CLEANING OPENING_DATE COLUMN')
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'],errors='coerce',infer_datetime_format=True,format='mixed')
        store_df['opening_date']=store_df['opening_date'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
        print(f"total number of NaN value in opening_date column: {store_df['opening_date'].isna().sum()}")
        
        print('CLEANING STORE_TYPE COLUMN') 
        print(f"Unique values:{store_df['store_type'].unique()}")
        store_df['store_type']= store_df['store_type'].astype('string')
        
        print('CLEANING LATITUDE COLUMN')
        store_df = store_df[pd.to_numeric(store_df['latitude'],errors='coerce').notnull()]
        store_df['latitude'] = store_df['latitude'].astype('float')
        print(f"Total number of NaN value in latitude column is: {store_df['latitude'].isna().sum()}")
        
        print('CLEANING COUNTRY_CODE COLUMN')
        print(f"Unique country code values: {store_df['country_code'].unique()}")
        store_df['country_code'] = store_df['country_code'].astype('string')
        number_of_country_code = store_df['country_code'].count()
        check_upper_string = store_df['country_code'].str.isupper().count()
        if check_upper_string == number_of_country_code:
            print("All data have the same format Uppercase")
        else:
            print("Update the data")
            
        print('CLEANING CONTINENT COLUMN')
        print(f"\n List of unique values in continent column:{store_df['continent'].unique()}")
        store_df['continent'] = store_df['continent'].astype('string')
               
        valid_continent_names = ['Europe', 'America']
        print(f"valid continents names are:{valid_continent_names}")
        store_df = store_df[store_df['continent'].isin(valid_continent_names)]
        print(f"\nChecking null values in dataframe:\n {store_df.isna().sum()}")
        
        store_df = store_df.reset_index()
        print(f"\nChecking NaN value in Store Dataframe afret reindexing:\n{store_df.isna().sum()}")
        print(store_df.info())
        
        connection = DatabaseConnector()
        upload_data =connection.upload_to_db(
            host=host,
           user=database_user,
           password=password,
           database_name=database_name,
           port=port,
           user_data=store_df,
           table_name=table_name
           )
        if True:
            print(f'TABLE {table_name} SUCCESSFULLY UPLOADED TO DATABASE {database_name}')
    
    def convert_product_weights(self,products_df:str):
        
        s3_data = DataExtractor().extract_from_s3(products_df)
        if True:
            print('AWS csv_s3-Data downloaded')
        print(s3_data.info())
        print(f'Checking Weight column for NaN values...')
        print(s3_data['weight'].isna().sum())
        print("Dropping NaN values in Weight Column...")
        s3_data.dropna(subset=['weight'], inplace=True)
        print(f"total NaN values after dropping: {s3_data['weight'].isna().sum()}")
        print("CLEANING WEIGHT COLUMN")
    
        def weight_update(weight):
            
            if 'kg' in weight:               
                kg_weight = weight
                return kg_weight
        
            elif 'g' and 'x' in weight:                
                x_g_weight = weight.replace('g','').replace('x','')
                split_weight = x_g_weight.split()
                format_weight = int(split_weight[0]) * int(split_weight[1])
                converted_weight = f'{format_weight / 1000}kg'
                return converted_weight
    
            elif 'g' in weight:               
                g_weight = weight.replace('g','').replace('.','')     
                new_weight = f'{float(g_weight) / 1000}kg'
                return new_weight
        
            elif 'ml' in weight:
                ml_weight = weight
                format_ml = ml_weight.replace('ml','')
                new_weight = Decimal(format_ml) /1000
                format_weights = f'{new_weight}kg'
                return format_weights               
            else:    
                return np.nan
               
        s3_data['weight'] = s3_data['weight'].apply(weight_update)
        
        def weight_to_float(weight):
            
            float_weight = str(weight).replace('kg','')
            return float_weight
        
        s3_data['weight'] = s3_data['weight'].apply(weight_to_float).astype('float')
        print(f"Checking number of NaN values in Weight column:{s3_data['weight'].isna().sum()}")
        s3_data.dropna(subset=['weight'],inplace=True)
        print(f"Checking number of NaN values in Weight column after droping values:{s3_data['weight'].isna().sum()}")
        print(s3_data.info())  
        return s3_data

    def clean_products_data(
        self,
        products_df:str,
        host:str,
        database_user:str,
        password:str,
        database_name:str,
        port:str,
        table_name:str
        ):
        '''
        This function take a Dataframe as argument and return a dataframe.
        Parameters:
        ------------
        products_df:String representing the s3 boto3 linkg to retreive the dataframe
        
        Return
        ----------
        Return a Dataframe
        '''
        s3_df = self.convert_product_weights(products_df)
        print(f"\tCleaning Product Name Column:\n{s3_df['product_name'].info()}")
        
        s3_df['product_name'] = s3_df['product_name'].astype('string')
        s3_df['product_name']=s3_df['product_name'].apply(lambda x: ''.join([char if ord(char) < 128 else '' for char in x])).astype('string')
        print(f"Total number of NaN values in product_name column is: {s3_df['product_name'].isna().sum()}")
        s3_df.dropna(subset=['product_name'],inplace=True)
        print(f"Total number of NaN values in product_name column after droping values is: {s3_df['product_name'].isna().sum()}")

    
        print("Finding column class")
        column_type_count = s3_df['product_price'].apply(type).value_counts()
        print(column_type_count)
        print('checking product_price currency values')
        
        def clean_currency_price(price):
            
            if '£' in price:               
                if isinstance(price, str):        
                    return price.replace('£','').replace(',','')
            else:
                np.nan                         
            return price
        
        s3_df['product_price']=s3_df['product_price'].apply(clean_currency_price).astype('string')
        s3_df['product_price']=s3_df['product_price'].str.extract('(-{0,1}\d+\.\d+)', expand=False).astype(float)
        print(f"Total number of NaN values in product price column :{s3_df['product_price'].isna().sum()}")
        s3_df.dropna(subset=['product_price'], inplace=True)
        print(f"Checking Nan values after droping in Product price column :{s3_df['product_price'].isna().sum()}")
        print('')
        print('Cleaning Category column')
        column_type_count = s3_df['category'].apply(type).value_counts()
        print(column_type_count)

        def format_category(data):
            
            if isinstance(data, str):                
                return data.replace('-',' ').title()
            else:
                np.nan
            return data

        s3_df['category']=s3_df['category'].apply(format_category).astype('string')
        print(f"Total number of NaN values in category column :{s3_df['category'].isna().sum()}")
        s3_df.dropna(subset=['category'], inplace=True)
        print(f"Checking Nan values after droping in Product price column :{s3_df['category'].isna().sum()}") 
          
        print('Checking for duplicated EAN number in')
        check_duplicates = s3_df['EAN'].duplicated().any()
        print(check_duplicates)
        s3_df['EAN'] = s3_df['EAN'].apply(lambda x: ''.join([number for number in str(x) if number.isnumeric()])).astype('int64')
        check_duplicates_ean = s3_df['EAN'].duplicated().sum()
        print(f'total number of duplicated EAN numbers is: {check_duplicates_ean}')
        print(f"Total number of NaN values in product price column :{s3_df['EAN'].isna().sum()}")
        s3_df.dropna(subset=['EAN'], inplace=True)
        print(f"Checking Nan values after droping in Product price column :{s3_df['EAN'].isna().sum()}")
        
        nan_values = s3_df['date_added'].isna().sum()
        print(f'Date_added total NaN values: {nan_values}')
        s3_df['date_added']= pd.to_datetime(s3_df['date_added'],errors='coerce',infer_datetime_format=True,format='mixed')
        print(f"Total number of NaN values in date_added column :{s3_df['date_added'].isna().sum()}")
        s3_df.dropna(subset=['date_added'], inplace=True)
        print(f"Checking Nan values after droping in date_added column :{s3_df['date_added'].isna().sum()}")
        
        check_duplicated= s3_df['uuid'].duplicated().sum()
        print(f'Total number of duplicated UUID is:{check_duplicated}')
        print(f"Total number of NaN values in UUID column :{s3_df['uuid'].isna().sum()}")
        s3_df.dropna(subset=['uuid'], inplace=True)
        print(f"Checking Nan values after droping in UUID column :{s3_df['uuid'].isna().sum()}")
        
        s3_df['removed']= s3_df['removed'].apply(lambda x: str(x).replace('Still_avaliable','Available')).astype('string')
        print(f"Total number of NaN values in removed column :{s3_df['removed'].isna().sum()}")
        s3_df.dropna(subset=['removed'], inplace=True)
        print(f"Checking Nan values after droping in Product price column :{s3_df['removed'].isna().sum()}")
        
        print("Checking Product code column")
        unique_value = s3_df['product_code'].is_unique
        if unique_value:
            print('Values are unique')
        else:
            print('Duplicated values')
        
        s3_df['product_code'] = s3_df['product_code'].apply(lambda x: str(x).upper()).astype('string')
        s3_df['product_code'] = s3_df['product_code'].apply(lambda x: str(x).replace('\W','')).astype('string')
        s3_df.dropna(subset=['product_code'], inplace=True)
        print(f"Checking total number of null values after cleaning: {s3_df['product_code'].isna().sum()}")
        
        s3_df = s3_df.dropna()
        print(s3_df.info())
        
        connection =  DatabaseConnector()
        upload_data =connection.upload_to_db(
            host=host,
            user=database_user,
            password=password,
            database_name=database_name,
            port=port,
            user_data=s3_df,
            table_name=table_name
            )
        if True:
            print(f'TABLE {table_name} SUCCESSFULLY UPLOADED TO DATABASE {database_name}')
            
        return s3_df


    def clean_orders_data(self,
                    order_table:str,
                    user_access:str,
                    database_type:str,
                    database_api:str,
                    host:str,
                    database_user:str,
                    password:str,
                    database_name:str,
                    port:str,
                    table_name:str
                    ):
        
        '''
        This function transform a table from a database to a pandas dataframe
        Parameters:
        -------------
        order_table: A string referencing the database table to convert into dataframe
        
        Return:
        Return a pandas dataframe
        '''
        orders_collected = DataExtractor()
        self.order_data = orders_collected.read_rds_table(order_table,user_access,database_type,database_api)
        if True:
            print("Database table succesfully converted to Pandas Dataframe")
            
        orders_df = self.order_data
        print(orders_df.info())
        orders_df.drop(['level_0','first_name', 'last_name','1'], axis=1,inplace=True)  
        orders_df.set_index('index',inplace=True)
        print("...Cleaning data...")
        
        orders_df['card_number']=orders_df['card_number'].apply(lambda x: x if len(str(x)) >=12 and len(str(x))<=19 else np.nan) #1
        orders_df['card_number']=pd.to_numeric(orders_df['card_number'],errors='coerce').fillna(0).astype('int64') #2
        orders_df['card_number'].isna().sum()
                
        print('Checking if all cards numbers are of type integer')        
        card_type_validation = is_numeric_dtype(orders_df['card_number'])
        if card_type_validation:
            print('Cards numbers in the column are numerics')
        else:
            print('Errors')  
        orders_df['store_code'] = orders_df['store_code'].apply(lambda x: str(x).upper()).astype('string')
        orders_df['store_code'] = orders_df['store_code'].apply(lambda x: str(x).replace('\W','')).astype('string')
        print(f"Checking total number of null values in store_code column after cleaning: {orders_df['store_code'].isna().sum()}")
        
        orders_df['product_code'] = orders_df['product_code'].apply(lambda x: str(x).upper()).astype('string')
        orders_df['product_code'] = orders_df['product_code'].apply(lambda x: str(x).replace('\W','')).astype('string')
        print(f"Checking total number of null values in product_code after cleaning: {orders_df['product_code'].isna().sum()}")
        
        orders_df = orders_df[pd.to_numeric(orders_df['product_quantity'],errors='coerce').notnull()]
        orders_df['product_quantity'] = orders_df['product_quantity'].astype('int64')
        print(f"Total number of NaN values in product_quantity column is: {orders_df['product_quantity'].isna().sum()}")
        print('') 
        product_quantity_validation = is_numeric_dtype(orders_df['product_quantity'])
        if product_quantity_validation:         
            print('The product quantity column contains only numerical values')
        else:        
            print('check data format')
    
        orders_df=orders_df.reset_index()
        print(orders_df.info())  
        connection =  DatabaseConnector()
        upload_data =connection.upload_to_db(
            host=host,
            user=database_user,
            password=password,
            database_name=database_name,
            port=port,
            user_data=orders_df,
            table_name=table_name
            )
        if True:
            print(f'TABLE {table_name} SUCCESSFULLY UPLOADED TO DATABASE {database_name}')
                 
        return upload_data
    
    def clean_dates_events(
        self,
        dates_data:str,
        host:str,
        database_user:str,
        password:str,
        database_name:str,
        port:str,
        table_name:str
        ):
        
        
        s3_data = DataExtractor()
        dates_df = s3_data.extract_json_from_s3(dates_data)
        if True:
            print("JSON s3 downloaded")
        print('Creating new pdSerie Sales_date and cleaning the column')
        dates_df['sales_date']=pd.to_datetime(dates_df.year.astype(str) + '/' + 
                                              dates_df.month.astype(str) + '/' 
                                              + dates_df.day.astype(str) + '/' 
                                              + dates_df.timestamp.astype(str),errors='coerce',infer_datetime_format=True)
        print(f"Checking NaN values in sales_data: {dates_df['sales_date'].isna().sum()}")
        dates_df.dropna(subset=['sales_date'],inplace=True)
        print(f"Checking NaN values after dropping values in sales_data: {dates_df['sales_date'].isna().sum()}")
        
        
        dates_df['time_period'] = dates_df['time_period'].astype('string')
        print(f"Checking NaN values in time_period column: {dates_df['time_period'].isna().sum()}")
        dates_df.dropna(subset=['time_period'],inplace=True)
        dates_df['time_period'] = dates_df['time_period'].str.replace('_',' ')
        print(f"Checking NaN values after dropping values in sales_data: {dates_df['time_period'].isna().sum()}")
        
        dates_df['timestamp']= dates_df['timestamp'].astype('string')
        print(f"Checking NaN values in timestamp column: {dates_df['timestamp'].isna().sum()}")
        dates_df.dropna(subset=['timestamp'],inplace=True)
        print(f"Checking NaN values after dropping values in sales_data: {dates_df['timestamp'].isna().sum()}")
        
        dates_df['month'] = dates_df['month'].astype('string')
        print(f"Checking NaN values in month column: {dates_df['month'].isna().sum()}")
        dates_df.dropna(subset=['month'],inplace=True)
        print(f"Checking NaN values after dropping values in sales_data: {dates_df['month'].isna().sum()}")
        
        
        dates_df['year'] = dates_df['year'].astype('string')
        print(f"Checking NaN values in month column: {dates_df['year'].isna().sum()}")
        dates_df.dropna(subset=['year'],inplace=True)
        print(f"Checking NaN values after dropping values in sales_data: {dates_df['year'].isna().sum()}")
        
        
        
        dates_df['day'] = dates_df['day'].astype('string')
        print(f"Checking NaN values in month column: {dates_df['day'].isna().sum()}")
        dates_df.dropna(subset=['day'],inplace=True)
        print(f"Checking NaN values after dropping values in sales_data: {dates_df['day'].isna().sum()}")
        
        dates_df.info()
        connection = DatabaseConnector()
        upload_data =connection.upload_to_db(
            host=host,
            user=database_user,
            password=password,
            database_name=database_name,
            port=port,
            user_data=dates_df,
            table_name=table_name
            )
        if True:
            print(f'TABLE {table_name} SUCCESSFULLY UPLOADED TO DATABASE {database_name}')
            
        return upload_data
    
    
