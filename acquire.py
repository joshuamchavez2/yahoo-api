import requests
import pandas as pd
from env import api_key
from datetime import timedelta, datetime

def get_data():

    

    url = 'https://yfapi.net/ws/screeners/v1/finance/screener/predefined/saved?count=250&scrIds=day_gainers'

    headers = {
        'x-api-key': api_key
        }

    # Request data
    response = requests.request("GET", url, headers=headers)

    # Save response to data
    data = response.json()

    # Creating an empty list to grab the symbols 
    list_of_symbols = []

    # Going to be looping through the list of quotes
    quotes = data['finance']['result'][0]['quotes']

    # Loop through quotes and save symbol to list
    for quote in quotes:
        list_of_symbols.append(quote['symbol'])

    # Grabbing the price of the most popular 250 stocks for the last 5 years

    df = pd.DataFrame()
    # Testing out only the first 10 stocks of list_of_stocks
    for y in range(10, 260, 10):
        
        # Resetting temp as an empty Data Frame
        temp = pd.DataFrame()
        
        # Setting x to 10 less than y
        x = y-10

        # Created an emtpy string
        placeholder = ""

        # Creating a placeholder string from list of symbols
        for index, symbol in enumerate(list_of_symbols[x:y]):

            # if index is equal to nine then this is last symbol and doesn't require a comma at the end
            if index == 9:
                placeholder += symbol

            # All other index's will require a comma
            else:
                placeholder += symbol + ","

        # Using requests to get the last 5 years of the 5 stocks saved under placeholder
        response = requests.request("GET", f'https://yfapi.net/v8/finance/spark?interval=1d&range=5y&symbols={placeholder}', headers=headers)
        data = response.json()

        # Using the function defined above to create a Data Frame
        temp = extract_date_and_price(data)
        df = pd.concat([df, temp], axis = 1)
    return df

def extract_date_and_price(data):
    '''
    This function takes in JSON file,
    creates a series for price and date,
    converts timestamp into a proper date,
    combines both price and date series into one temp dataframe,
    convert data to datetime64 and set as index
    merge to the main dataframe
    returns the main dataframe
    '''
    # Creating an empty main DataFrame
    df = pd.DataFrame()

    # Looping through each symbol in data
    for symbol in data:
        
        # Creating an empty temporary dataframe
        temp = pd.DataFrame()
        
        # Creating a series for timestamp and closing price
        timestamp = pd.Series(data[symbol]['timestamp'])
        price = pd.Series(data[symbol]['close'])

        # Converting timestamp to year/month/day format
        timestamp = timestamp.apply(lambda x : datetime.utcfromtimestamp(x).strftime('%Y-%m-%d'))

        # Saving series into a the empty DataFrame I created earlier
        temp[symbol + '_price'] = price
        temp['date'] = timestamp

        # Converting type object to datetime64
        temp['date'] = pd.to_datetime(temp.date)
        
        # Using the date as the index
        temp = temp.set_index('date').sort_index()
        
        # Saving the temporary DataFrame to the main DataFrame
        df = pd.merge(df, temp, how='outer', left_index=True, right_index=True)
        
    return df