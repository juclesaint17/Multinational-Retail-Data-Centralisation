from sqlalchemy import text, inspect,create_engine
from database_utils import DatabaseConnector
import pandas as pd
import tabula
import json
import requests
import time


class DataExtractor:
    
    def __init(self):
        
        self.total_number_of_stores = 0
        
        
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
                print(f"\t LIST OF USERS DATA{user_data}")
                print("")
                return user_data
        except Exception as error:
            print("FAILED TO CONNECT TO THE DATABASE...")       
         
         
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
            print("\tREADING PDF DATA FILES...")
            card_data = tabula.read_pdf(data_url, pages='all')
            print("\tPDF DATA COLLECTED...")
            return card_data
        except Exception as error:
            print("ERROR READING DATA")
        
        
        
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
        try:
            
            response = requests.get(number_of_stores_url,headers=header_keys)
            if response.status_code ==200:
                stores_numbers= response.json()
                total_number_of_stores = stores_numbers
                print(f"The total number of stores is: {total_number_of_stores['number_stores']}")
    
        except Exception as error:       
            print(f"response failed with status code: {response.status_code}")
            print(f"Response Text:{response.text}")
                
        
        return total_number_of_stores['number_stores']
    
        
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
        list_stores_data = []
        for store_index in range(total_number_of_stores):
            store_number = store_index
            try:
                response = requests.get(store_url+str(store_number),headers=api_keys)   
                if response.status_code == 200:
                    store_details = response.json()
                    if store_details:
                        print(f'STORE : {store_number} RETRIEVED SUCCESSFULLY.')
                        
                    else:
                        print('DATA NOT RETRIEVED..')
                    list_stores_data.append(store_details)
                    
                    print(f"DATA SUCCESSFULLY SAVED IN {csv_file_path} FILE")
        
            except Exception as error:        
                print(f"Response with store number {store_number} failed with status code:{response.status_code}")
                print(response.text)
                time.sleep(1)
        print("CREATING DATAFRAME...")        
        stores_details_df = pd.DataFrame(list_stores_data)
        if True:
            print("DATAFRAME CREATED.")
            time.sleep(1)
            stores_details_df.to_csv(csv_file_path)
            if True:
                print(f"CSV FILE: {csv_file_path} SUCCESSFULLY CREATED")               
                return csv_file_path
    
 
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
        try:
            s3_df_products = pd.read_csv(csv_s3_address,index_col=0)
            print('\tLIST OF PRODUCTS')
            return s3_df_products
        except Exception as error:
            print("FAILED TO EXTRACT DATA FROM S3 LINK")
       
        
    
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
        try:
            s3_df_product = pd.read_json(json_s3_addresss3_address)
            return s3_df_product
        except Exception as error:
            print("FAILED TO EXTRACT DATA FROM JSON LINK")
        
            
            
