import requests
import pandas as pd
from env import api_key
from datetime import timedelta, datetime

def get_data():
    '''
    This functions brings all other functions together
    First we grab all the top gainers for the day callign get_day_gainers
    Checks to see how many total stocks there are 
    API can get prices of 10 total per request so we bundle them up by 10
    Request 10 at time and load them into the main dataframe df
    return df
    '''
    # Set headers to include my api_key
    headers = {
        'x-api-key': api_key
        }

    # Get day gainers
    list_of_symbols = get_day_gainers()

    # Create an emtpy DataFrame
    df = pd.DataFrame()

    # Check the size of the todays top gainers list
    total = len(list_of_symbols)

    # Create a variable named end to be where the range will end
    end = ((total//10)*10)+10

    # Check to see if total is evenly divisible by 10.  This is because api can grab 10 symbols per request
    if total%10 !=0:

        # If the total is not evenly divisible by 10, loop through list by 10s, then grab the ones that are left later
        for y in range(10, end, 10):
            
            # Setting x to be 10 less than y
            x = y-10
            
            # Creating a string called placeholder to be from range x:y (groups of 10) in the long list of symbols
            placeholder = create_placeholder(list_of_symbols[x:y])
            
            # Requesting all 10 stocks last 5 years of prices
            response = requests.request("GET", f'https://yfapi.net/v8/finance/spark?interval=1d&range=5y&symbols={placeholder}', headers=headers)
            
            # Save the response to data
            data = response.json()
            
            # Creating an empty temporary Data Frame
            temp = pd.DataFrame()

            # Extracting the prices from json file
            temp = extract_date_and_price(data)

            # Adding the 10 stocks last 5 years to the main DataFrame df
            df = pd.concat([df, temp], axis = 1)
        
        # Clearing temp
        temp = pd.DataFrame()

        # Grabbing the rest, should be less than 10 stocks symbols left
        placeholder = create_placeholder(list_of_symbols[end-10:total])

        # Repeating the process to get the stock information for the last remaining symbols
        response = requests.request("GET", f'https://yfapi.net/v8/finance/spark?interval=1d&range=5y&symbols={placeholder}', headers=headers)
        
        # Saving response to data again
        data = response.json()

        # Extracting once more
        temp = extract_date_and_price(data)

        # Adding the final symbols to the main dataframe df
        df = pd.concat([df, temp], axis = 1)

    else:

        # If total is evenly divisible by 10, no need to add the extra step of getting the rest
        # Just Loop by 10
        for y in range(10, end, 10):
            
            # Setting x to be 10 less than y
            x = y-10
            
            # Creating a string called placeholder to be from range x:y (groups of 10) in the long list of symbols
            placeholder = create_placeholder(list_of_symbols[x:y])
            
            # Requesting all 10 stocks last 5 years of prices
            response = requests.request("GET", f'https://yfapi.net/v8/finance/spark?interval=1d&range=5y&symbols={placeholder}', headers=headers)
            
            # Save the response to data
            data = response.json()
            
            # Creating an empty temporary Data Frame
            temp = pd.DataFrame()

            # Extracting the prices from json file
            temp = extract_date_and_price(data)

            # Adding the 10 stocks last 5 years to the main DataFrame df
            df = pd.concat([df, temp], axis = 1)

    return df 

def create_placeholder(list_of_symbols):
    '''
    This function will take in a list of symbols
    Creates a single string with no spaces only commas
    Returns thats string
    '''
    
    # Creating an empty string
    placeholder = ""

    # Creating a placeholder string from list of symbols
    for index, symbol in enumerate(list_of_symbols):

        # if index is equal to nine then this is last symbol and doesn't require a comma at the end
        if index == 9:
            placeholder += symbol

        # All other index's will require a comma
        else:
            placeholder += symbol + ","

    return placeholder

def get_day_gainers():
    '''
    This function uses the yahoo api to grab the top 250 day_gainers.
    The API returns a json with a quote information about each stock
    Loop through json file to extract just the stock symbol for each stock
    Save to list and return that list
    '''
    
    # defining the url to make the request too
    url = 'https://yfapi.net/ws/screeners/v1/finance/screener/predefined/saved?count=250&scrIds=day_gainers'

    # passing in api_key
    headers = {
        'x-api-key': api_key
        }

    # Sending Request
    response = requests.request("GET", url, headers=headers)
    
    # Saving request response to data
    data = response.json()
    
    # Creating an empty list to grab the symbols 
    list_of_symbols = []

    # Going to be looping through the list of quotes
    quotes = data['finance']['result'][0]['quotes']

    # Loop through quotes and save symbol to list
    for quote in quotes:
        list_of_symbols.append(quote['symbol'])
    
    return list_of_symbols

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