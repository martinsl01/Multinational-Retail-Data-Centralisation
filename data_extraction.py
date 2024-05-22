import pandas as pd
from database_utils import DatabaseConnector as dbc
from tabula import read_pdf
import requests
import json
import boto3

class DataExtractor: 

    """Extracts data from tables, converting it to pandas dataframe"""

    def read_rds_table(self, table_name, engine):
        engine = engine.connect()
        rds_db = pd.read_sql_table(table_name, engine)
        return rds_db
    
    def retrieve_pdf_data(self, pdf_path):

        pdf_table = read_pdf(pdf_path, pages='all')
        pdf_df = pd.concat(pdf_table)
        return pdf_df

    def list_number_of_stores(self, endpoint, header):
        stores_number = requests.get(endpoint, headers=header)
        number = stores_number.json()
        number_stores = number['number_stores']
        return number_stores
    
    def retrieve_stores_data(self, number_of_stores, endpoint, header):
        store_list = []
        for store in range(0, number_of_stores):
            store_info = requests.get(f'{endpoint}{store}', headers=header)
            store_data = store_info.json()
            store_list.append(store_data)
            
        store_table = pd.DataFrame(store_list)
        return store_table
    
    def extract_from_s3(self, s3_address):
        s3 = boto3.resource('s3')
        if 'https://' in s3_address:
            s3_address = s3_address.replace('https://','')

        bucket_name, key = s3_address.split('/', 1)
        bucket_name = 'data-handling-public'
        s3_object = s3.Object(bucket_name, key)
        body = s3_object.get()['Body']
        products_df = pd.read_json(body)
        
        return products_df
