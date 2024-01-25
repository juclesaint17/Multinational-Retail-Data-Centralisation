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
        user_table
        )
    print(user_cleaning)
    
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
        
    
if __name__== '__main__':
    print("STARTING")
    loads_transform_data()
    print("UPDATE COMPLETED")

    
    



    

   
