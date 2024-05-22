import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect
from database_utils import DatabaseConnector as dbc
from data_extraction import DataExtractor as extract
from data_cleaning import DataCleaning as clean

def ETL():
    extract()
    clean()
    dbc()






db_connector = dbc() 
ext = extract()
cl = clean()

# Uploading User data to DB
file = 'db_creds.yaml'
database_cred = db_connector.read_db_cred(file)
engine = db_connector.init_db_engine(database_cred)
list_of_tables = db_connector.list_db_tables(engine)
users = list_of_tables[1]
users_table = ext.read_rds_table(users, engine)
users_info = cl.clean_user_data(users_table)
# upload = db_connector.upload_to_db(users_info, engine)
# upload  
# print(users_info.info())
# temp = users_info['date_of_birth'].tolist()
# print(set(temp))

# Uploading Card_data_tabe to DB
pg_creds = db_connector.read_pg_cred()
connect = db_connector.connect_to_pg(pg_creds)
path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
# card_data = ext.retrieve_pdf_data(path)
# card_data_table = cl.clean_card_data(card_data)
# upload_data = db_connector.upload_to_db(card_data_table, connect)
# upload_data

# print(card_data_table)


# Uploading store data to DB

endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
number_of_stores = ext.list_number_of_stores(endpoint, header)

url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/0'
stores_table = ext.retrieve_stores_data(number_of_stores, url, header)
stores = cl.clean_store_data(stores_table)
# upload_stores = db_connector.upload_to_db(stores, connect)
# upload_stores
#temp_list = stores['store_type'].tolist()
#print(set(temp_list))

# Converting product weight, cleaning and uploading products data to DB

#address = 's3://data-handling-public/products.csv'
#product_info = ext.extract_from_s3(address)
#products = cl.convert_product_weights(product_info)
#products_table = cl.clean_products_data(products)
#upload_products = db_connector.upload_to_db(products_table, connect)

#print(products_table.info())
#temp_list = products_table['EAN'].tolist()
#print(set(temp_list))

'''Upload orders table to the database'''
#database_cred = db_connector.read_db_cred()
#engine = db_connector.init_db_engine(database_cred)
#list_of_tables = db_connector.list_db_tables(engine)
#orders = list_of_tables[2]
#orders_table = ext.read_rds_table(orders, engine)
# orders_df = cl.clean_orders_data(orders_table)
# upload_orders = db_connector.upload_to_db(orders_df, connect)
# upload_orders

# Extracting date_events_data

address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
date_events_data = ext.extract_from_s3(address)
events_data = cl.clean_sales_date(date_events_data)
print(events_data)
# upload_events = db_connector.upload_to_db(events_data, connect)
# upload_events 

# temp = events_data['day'].tolist()
# print(set(temp))