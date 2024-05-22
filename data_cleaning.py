from data_extraction import DataExtractor as de
from database_utils import DatabaseConnector as dbc
import pandas as pd
import numpy as np
from dateutil.parser import parse
import re


class DataCleaning:
    def clean_user_data(self, legacy_users):

        '''
        Cleans the user data by removing duplicates where necessary, changing columns to the correct 
        datetype, drops redundant columns and filters out jargon 
        '''
        legacy_users = legacy_users.drop_duplicates(subset=['email_address'])
        legacy_users = legacy_users.drop_duplicates(subset=['phone_number'])
        legacy_users = legacy_users.drop_duplicates(subset=['user_uuid'])
        legacy_users['country_code'] = legacy_users['country_code'].str.replace('GGB', 'GB')
        filt_cc = ['GB', 'US', 'DE']
        legacy_users = legacy_users.loc[legacy_users['country_code'].isin(filt_cc)]
        legacy_users['join_date'] = pd.to_datetime(legacy_users['join_date'], errors='coerce')
        legacy_users['date_of_birth'] = pd.to_datetime(legacy_users['date_of_birth'], errors='coerce')
        legacy_users['country'] = legacy_users['country'].astype("category")
        legacy_users['first_name'] = legacy_users['first_name'].astype("string")
        legacy_users['last_name'] = legacy_users['last_name'].astype("string")
        legacy_users.drop(legacy_users.columns[0], axis=1, inplace=True)
        regex_expression = r'^(?!(((\+44\s?\d{4}|\(?0\d{4}\)?)\s?\d{3}\s?\d{3})|((\+44\s?\d{3}|\(?0\d{3}\)?)\s?\d{3}\s?\d{4})|((\+44\s?\d{2}|\(?0\d{2}\)?)\s?\d{4}\s?\d{4}))(\s?#(\d{4}|\d{3}))?$).+'
        legacy_users.loc[legacy_users['phone_number'].str.match(regex_expression), 'phone_number'] = None
        legacy_users['phone_number'] = legacy_users['phone_number'].replace({r'\+44': '0', r'\(': '', r'\)': '', r'-': '', r' ': ''}, regex=True)
        legacy_users['phone_number'] = legacy_users['phone_number'].astype("string", errors='ignore')
        

        
        return legacy_users
    
    def clean_card_data(self, card_data):

        '''
        Cleans the card data by removing duplicates where necessary, changing columns to the correct 
        datetype, drops redundant columns and filters out jargon 
        '''
        card_data['card_number'] = card_data['card_number'].replace({r'\?': ''}, regex=True)
        card_data = card_data[~card_data['card_number'].str.contains('[a-zA-Z?]', na=False)]
        card_data['card_number'] = card_data['card_number'].astype("int64")
        regex_exp = r'^((0?[1-9]|1[0-2])\/\d{2})$/gm'
        card_data.loc[card_data['expiry_date'].str.match(regex_exp), 'expiry_date'] = None
        card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], errors='ignore')
        card_data['card_provider'] = card_data['card_provider'].astype("string")  
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], errors='coerce')
        card_data = card_data.drop_duplicates(subset=['card_number'])
        valid_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
       'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
       'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
        card_data = card_data.loc[card_data['card_provider'].isin(valid_providers)] 
        card_data.dropna(subset=['card_number'], how='any', axis=0, inplace=True)
        card_data.dropna(subset=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed'], how='all', axis=0, inplace=True)


        return card_data
    

    def clean_store_data(self, store_data):

        '''
        Cleans data by changing columns to the correct datetype, drops redundant columns
        and filters out jargon 
        '''

        store_data = store_data[~store_data['longitude'].str.contains('[a-zA-Z?]', na=False)]
        store_data = store_data[~store_data['staff_numbers'].str.contains('[a-zA-Z?]', na=False)]
        store_data.dropna(subset=['address'], how='any', axis=0, inplace=True)
        store_data['continent'] = store_data['continent'].str.replace('eeEurope', 'Europe').str.replace('eeAmerica', 'America')
        filter_cont = ['Europe', 'America']
        store_data = store_data.loc[store_data['continent'].isin(filter_cont)]
        store_data['opening_date'] = store_data['opening_date'].apply(parse)
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], infer_datetime_format=True, errors='coerce')
        store_data = store_data.drop('lat', axis = 1)
        store_data = store_data.drop('index', axis = 1)
        filter_cc = ['GB', 'US', 'DE',]
        store_data = store_data.loc[store_data['country_code'].isin(filter_cc)]
        store_data['longitude'] = store_data['longitude'].astype("float64")
        store_data['latitude'] = store_data['latitude'].astype("float64")
        store_data['staff_numbers'] = store_data['staff_numbers'].astype("int64")
    
        return store_data

    
    def convert_product_weights(self, prod_weight): 

        ''' The line of code below cleans the weight column by converting other unites into kg
        '''

        # Dealing with values with x that need to be multiplied
        prod_weight['weight'] = prod_weight['weight'].str.replace('12 x 100g', '1200g').str.replace('12 x 85g', '1020g').str.replace('8 x 150g', '1200g').str.replace('3 x 90g', '270g').str.replace('3 x 132g', '396g').str.replace('4 x 400g', '1600g').str.replace('6 x 400g', '2400g').str.replace('6 x 412g', '2472g').str.replace('16 x 10g', '160g').str.replace('5 x 145g', '725g').str.replace('3 x 2g', '6g').str.replace('8 x 85g', '680g').str.replace('2 x 200g', '400g').str.replace('40 x 100g', '400g')

        # Conversion ratio
        conversion = {'kg': 1, 'g': 0.001, 'ml': 0.001, 'oz': 0.035}

        # regex to remove anything that doesn't have a letter at the end of the string
        prod_weight['weight'] = prod_weight['weight'].str.replace(r'\s+\.', '', regex=True)


        for index, weight in prod_weight['weight'].items():
            for unit, multiplier in conversion.items():
                if isinstance(weight, str) and unit in weight:

                    number = float(weight.replace(unit, '').strip())
                    prod_weight.at[index, 'weight'] = round(number * multiplier, 3)
                    break

        return prod_weight       


    def clean_products_data(self, products_data):

        '''
        Cleans the prodtcts data by removing duplicates where necessary, changing columns to the correct 
        datetype, drop redundant columns and filter out jargon 
        '''

        Category_filt = ['food-and-drink', 'toys-and-games', 'homeware', 'pets', 'diy', 'sports-and-leisure', 'health-and-beauty']
        products_data = products_data.loc[products_data['category'].isin(Category_filt)]
        products_data = products_data.drop('Unnamed: 0', axis=1)
        products_data = products_data.rename(columns={"weight": "weight(kg)"})       
        products_data['weight(kg)'] = products_data['weight(kg)'].astype('float64')
        products_data = products_data[products_data['product_price'].notna()]
        products_data['product_price'] = products_data['product_price'].str.replace('£','')
        products_data['product_price'] = products_data['product_price'].astype('float64')
        products_data = products_data.rename(columns={"product_price": "product_price(£)"})
        products_data['date_added'] = pd.to_datetime(products_data['date_added'], errors='coerce')
        products_data = products_data[~products_data['EAN'].str.contains('[a-zA-Z?]', na=False)]
        products_data['EAN'] = products_data['EAN'].astype('int64')

        return products_data

    def clean_orders_data(self, orders_data):

        '''Dropes redeundant columns'''

        orders_data = orders_data.drop('first_name', axis=1)
        orders_data = orders_data.drop('last_name', axis=1)
        orders_data = orders_data.drop('1', axis=1)
        orders_data = orders_data.drop('index', axis=1)

        return orders_data

    def clean_sales_date(self, orders_date):

        '''
        Cleans the orders data by changing columns to the correct datetype, drop redundant columns, 
        filtering jargon as well as concatenating rows that require being joined together
        '''

        filt_period = ['Midday', 'Evening', 'Morning','Late_Hours']
        orders_date = orders_date.loc[orders_date['time_period'].isin(filt_period)]
        # orders_date['exact_date'] = orders_date[['year','month', 'day']].agg('-'.join, axis=1) 
        # orders_date = orders_date.drop('year', axis=1)
        # orders_date = orders_date.drop('month', axis=1)
        # orders_date = orders_date.drop('day', axis=1)
        # orders_date['date'] = orders_date['exact_date'] + ' ' + orders_date['timestamp']
        # orders_date = orders_date.drop('timestamp', axis=1)
        # orders_date = orders_date.drop('exact_date', axis=1)
        # orders_date['date'] = pd.to_datetime(orders_date['date'], errors='ignore')
        # orders_date['time_period'] = orders_date['time_period'].astype('string') 
        
        return orders_date