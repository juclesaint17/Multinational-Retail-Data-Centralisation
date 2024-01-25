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
        This function extract the database tables list them,and 
        convert it into pandas dataframe,store the table as cvs file(optional)
        
        Parameters:
        --------------
        table_name: 
        String referencing the database table to retrieve available tables
        
        Return:
        ----------
        Return a pandas Dataframe
        '''
        
        db_initialisation = DatabaseConnector()
        engine = db_initialisation.init_db_engine(user_access,database_type,database_api)
    
        with engine.connect() as conn:
            user_data = pd.read_sql_table(table_name=table_name,con=conn)
            return user_data
            
         
    def retrieve_pdf_data(self,data_url:str):       
        '''
        This function accept a tabular or PDF data url link and return
        a pandas dataframe
        Parameters:
        ---------------
        data_url: PDF file of the Tabular link
        
        Returns:
        -------
        Return a Pandas Dataframe file.
        
        '''
        card_data = tabula.read_pdf(data_url, pages='all')
        return card_data
    
        
    def list_number_of_stores(
        self,
        number_of_stores_url:str,
        header_keys:dict
        ):    
        
        '''
        This function connect to the API and return the total number of available stores in the end point
        Parameters:
        ------------
        number_of_stores_url: the url end poin to collect the total number of stores
        
        header_keys: API key authorisation key:value pair Dict()
        
        Return:
        Return the total number of stores
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
    
        
    def retrieve_stores_data(
        self,
        store_number_url:str,
        store_url:str,
        api_keys:dict,
        csv_file_path:str
        ):
        
        '''
        This function list the total number of stores and retrives each store data as a JSON file,convert the file to a dataframe 
        and store it into a CSV file localy
        
        Parameters
        -------------
        store_number_url: Url link to read the total number of available stores.
        store_url: Url link to extract stores data
        api_keys: the headers  access keys.
        cvs_file_path: the file path to store the collect data as csv file
        
        Return
        ----------
        Return a Dataframe
        '''
        
        total_number_of_stores = self.list_number_of_stores(store_number_url,api_keys)
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
    
 
    def extract_from_s3(self,csv_s3_address:str):
        '''
        This function extract a csv data from AWS s3 bucket and
        return a pandas dataframe
        Parameters:
        -----------------
        csv_s3_address: String referencing the address of the csv s3 bucket
        
        Return:
        --------
        return a pandas Dataframe
        '''
        s3_df_products = pd.read_csv(csv_s3_address,index_col=0)
        
        return s3_df_products
    
    
    def extract_json_from_s3(self,json_s3_addresss3_address:str):
        '''
        This function extract a json data from AWS s3 bucket and
        return a pandas dataframe
        Parameters:
        -----------------
        json_s3_address: String referencing the address of the json s3 bucket
        
        Return:
        -------
        return a pandas Dataframe
        '''
        s3_df_product = pd.read_json(json_s3_addresss3_address)
        return s3_df_product
        
        
 

        
        
    
    