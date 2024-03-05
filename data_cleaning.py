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
        
        user_data.set_index('index',inplace=True)
        print('USERS DATA TO CLEAN')
        print(user_data.info())
        time.sleep(1)
        print("CLEANING DIM_USERS_TABLES")
        user_data['first_name']=user_data['first_name'].astype('string')
        user_data['first_name']=user_data['first_name'].apply(lambda x: ''.join([char if ord(char) < 128 else '' for char in x])).astype('string')
        user_data['first_name']=user_data['first_name'].apply(lambda x: re.sub(r'\W+', '', x)).astype('string')
        user_data['first_name'] = user_data['first_name'].astype('string').str.replace('\d+','')
        
        user_data['last_name']=user_data['last_name'].astype('string')
        user_data['last_name']=user_data['last_name'].apply(lambda x: ''.join([char if ord(char) < 128 else '' for char in x])).astype('string')
        user_data['last_name']=user_data['last_name'].apply(lambda x: re.sub(r'\W+', '', x)).astype('string')
        user_data['last_name'] = user_data['last_name'].astype('string').str.replace('\d+','')
        
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'],errors='coerce',infer_datetime_format=True,format='mixed')
        user_data['date_of_birth']=user_data['date_of_birth'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
        print(f"Checking NaN values in Date of Birth Column: {user_data['date_of_birth'].isna().sum()}")
        
        user_data['company'] = user_data['company'].astype('string')
        user_data['company']=user_data['company'].apply(lambda x: re.sub(r'\W', ' ', x).title()).astype('string')
        user_data['company']=user_data['company'].apply(lambda x: x if isinstance(x, str)else np.nan).astype('string')
        
        user_data['email_address'] = user_data['email_address'].astype('string')
        def check_email_format(emails):
            
            if "@" in emails:   
                return emails
            else:
                np.nan         
        user_data['email_address'] = user_data['email_address'].apply(check_email_format).astype('string')
        
        user_data['address'] = user_data['address'].astype('string')
        user_data['address'] = user_data['address'].apply(lambda x: re.sub(r'[^\w\s]', '', x).title()).astype('string')
        def remove_special_characters(s): 
            return re.sub(r'[^a-zA-Z0-9\s]', '', str(s)) 
        #Apply the function to the column 
        user_data['address']=user_data['address'].apply(remove_special_characters).astype('string')
        user_data['address'] = user_data['address'].apply(lambda x: " ".join(x.split())).astype('string')
        
        uniques_countries = ['Germany', 'United Kingdom', 'United States']
        user_data = user_data[user_data['country'].isin(uniques_countries)]
        user_data['country'] = user_data['country'].astype('string')
        
        user_data['country_code'] = user_data['country_code'].astype('string')
        user_data['country_code'] = user_data['country_code'].str.replace('GGB','GB')
        unique_codes =['DE','GB','US']
        user_data['country_code']=user_data['country_code'].apply(lambda x: x if x in unique_codes else np.nan)
        user_data['country_code'] = user_data['country_code'].astype('string')
        
        user_data['phone_number'] = user_data['phone_number'].apply(lambda x: ''.join([number for number in str(x) if number.isnumeric()])).astype('int64')
        
        user_data['join_date']= pd.to_datetime(user_data['join_date'],errors='coerce',infer_datetime_format=True,format='mixed')
        user_data['join_date']=user_data['join_date'].apply(lambda x: x if isinstance(x, datetime.datetime) else np.nan)
        
        user_data.reset_index(inplace=True)
        print(user_data.info())

        print(f"UPLOADING CLEANING DATA TO THE DATABASE TABLE {table_name} OF {database_name} DATABASE..")
        
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
        products_dim = DataExtractor().extract_from_s3(products_df)
        if True:
            print('AWS csv_s3-Data downloaded')
        print(products_dim.info())
        #s3_df = self.convert_product_weights(products_df)
        print("CLEANING PRODUCTS DATA")
        products_dim['date_added']=pd.to_datetime(products_dim['date_added'],errors='coerce',infer_datetime_format=True,format='mixed')
        
        print(f"\tChecking unique products availability: \n{products_dim['removed'].unique()}")
        print("\tFormating unique availability")
        products_status = ['Still_avaliable', 'Removed']
        print(products_status)
        products_dim = products_dim[products_dim['removed'].isin(products_status)]
        products_dim['removed'] = products_dim['removed'].astype('string')
        
        products_dim['product_code']=products_dim['product_code'].apply(lambda x: str(x).replace('\W','')).astype('string')
        
        products_dim['EAN']=products_dim['EAN'].str.replace(r'[^0-9]+', '').astype('int64')
        EAN_validation = is_numeric_dtype(products_dim['EAN'])
        if EAN_validation:            
            print('The EAN column contains only numerical values')
        else:        
            print('check data format')
            
            
        print('Cleaning Category column')
        column_type_count = products_dim['category'].apply(type).value_counts()
        print(column_type_count)
        
        def format_category(data):              
            if isinstance(data, str):
                            
                return data.replace('-',' ').title()        
        products_dim['category']=products_dim['category'].apply(format_category).astype('string')
        products_dim['category'] = products_dim['category'].str.replace(r'[^A-Za-z]+', '')
        
        
        column_type_count = products_dim['product_price'].apply(type).value_counts()
        print(column_type_count)
        print('checking product_price currency values')          
        def clean_currency_price(price):
            
            if '£' in price:                 
                if isinstance(price, str): 
                    return price.replace('£','').replace(',','')        
        products_dim['product_price']=products_dim['product_price'].apply(clean_currency_price).astype('float')
        
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
  
        products_dim['weight']=products_dim['weight'].apply(weight_update)

        def weight_string(weight):  
            weight = str(weight).replace('kg','')
            return weight 
        products_dim['weight']=products_dim['weight'].apply(weight_string)
        products_dim['weight']=products_dim['weight'].str.replace(r'[^0-9\.\,]+', '')
        products_dim['weight']=products_dim['weight'].astype('string')
        
        products_dim['product_name'] = products_dim['product_name'].astype('string')
        products_dim['product_name']=products_dim['product_name'].apply(lambda x: ''.join([char if ord(char) < 128 else '' for char in x])).astype('string')
        products_dim['product_name']=products_dim['product_name'].apply(lambda x: x.replace("'",'')).astype('string')
        
        def remove_last_quote(name):
            for words in name.split():
                if words.endswith("'"):
                    updated_word=words.replace("'", '')
                    #return a
                    return updated_word
        products_dim['product_name'].apply(remove_last_quote)
        print(products_dim.info())
        
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
                    user_data=products_dim,
                    table_name=table_name
                    )
                
                print(f"\tTABLE {table_name} OF DATABASE: {database_name} UPDATED")
                               
                return f"PRODUCTS DATA TABLE: \n{products_dim}"
        except Exception as error:
            print("CONNECTION TO THE DATABASE FAILED")
        
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
        
        orders_df['card_number']=orders_df['card_number'].apply(lambda x: ''.join([number for number in str(x) if number.isnumeric()]))
        orders_df['card_number'] = orders_df['card_number'].str.replace(r'\D+', '')
        orders_df['card_number']=orders_df['card_number'].str.replace(r'[^0-9]+', '')
        orders_df['card_number'] = orders_df['card_number'].astype('int64')
        
        orders_df['store_code']=orders_df['store_code'].apply(lambda x: str(x).replace('\W','')).astype('string')
        number_of_store = orders_df['store_code'].count()
        store_code_format = orders_df['store_code'].str.upper().count()
        if store_code_format == number_of_store:        
            print('All data in store_code column are formatted in Uppercase format. ')
        else:
            print('please check the data')
        orders_df['store_code'] = orders_df['store_code'].astype('string')
        orders_df['store_code'] = orders_df['store_code'].apply(lambda x: str(x).upper()).astype('string')
        
        orders_df['product_code']=orders_df['product_code'].apply(lambda x: str(x).replace('\W','')).astype('string')
        orders_df['product_code'] = orders_df['product_code'].apply(lambda x: str(x).upper()).astype('string')
        
        orders_df['product_quantity'] = orders_df['product_quantity'].replace(r'[^0-9]+', '').astype('int64')
        orders_df=orders_df.reset_index()
        print(orders_df.info())  
        
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
                    user_data=orders_df,
                    table_name=table_name
                    )
                
                print(f"\tTABLE {table_name} OF DATABASE: {database_name} UPDATED")
                               
                return f"ORDERS DATA TABLE: \n{orders_df}"
        except Exception as error:
            print("CONNECTION TO THE DATABASE FAILED")
        
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
        dates_times = s3_data.extract_json_from_s3(dates_data)
        if True:
            print("s3_JSON FILE DOWNLOADED")
        
        print(dates_times.info())
        unique_time_period = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        dates_times = dates_times[dates_times['time_period'].isin(unique_time_period)]
        dates_times['time_period'] = dates_times['time_period'].astype('string')
        print(dates_times['time_period'].unique())
        
        
        print('\tCreating new pdSerie Sales_date and cleaning the column')
        dates_times['sales_date']=pd.to_datetime(dates_times.year.astype(str) + '/' + 
                                              dates_times.month.astype(str) + '/' 
                                              + dates_times.day.astype(str) + '/' 
                                              + dates_times.timestamp.astype(str),errors='coerce',infer_datetime_format=True)
        print(f"Checking NaN values in sales_data: {dates_times['sales_date'].isna().sum()}")
        
        dates_times['day']=dates_times['day'].str.replace(r'[^0-9]+', '').astype('string')
        dates_times['year']=dates_times['year'].str.replace(r'[^0-9]+', '').astype('string')
        dates_times['month']=dates_times['month'].str.replace(r'[^0-9]+', '').astype('string')
        dates_times['timestamp']= dates_times['timestamp'].str.replace(r'[^0-9]+','').astype('string')
        print(dates_times.info())
        
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
                    user_data=dates_times,
                    table_name=table_name
                    )
                
                print(f"\tTABLE {table_name} OF DATABASE: {database_name} UPDATED")
                               
                return f"DATES_TIME DATA TABLE: \n{dates_times}"
        except Exception as error:
            print("CONNECTION TO THE DATABASE FAILED")
        
    
    
