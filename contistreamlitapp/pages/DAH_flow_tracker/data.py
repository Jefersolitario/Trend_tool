import requests
import time
import pandas as pd
import certifi
from pymongo import MongoClient
import requests

def mongo_client():
    return MongoClient(
            f"mongodb://app_power_dashboard:jQl6TZMfYe61Brbs@dev1-shard-00-00.uvhb7.mongodb.net:27017,dev1-shard-00-01.uvhb7.mongodb.net:27017,dev1-shard-00-02.uvhb7.mongodb.net:27017/test?authSource=admin&replicaSet=atlas-k6fhv2-shard-0&ssl=true",
            tz_aware=True,
            w="majority",
            readpreference="primary",
            journal=True,
            wTimeoutMS=60000,
            connect=False,
            tlsCAFile=certifi.where(),
            maxPoolSize=200,
        )


client = mongo_client()


def get_ts_hot_actuals(start_date, end_date, config, meta_id, name):


    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M")

    url = config['timeseries']['endpoint']
    headers = {'accept': 'application/json'}
    params = {'meta_data_id': meta_id, 'event_at_utc_from': start_date, 'event_at_utc_to': end_date}
    username = config['timeseries']['user']
    password = config['timeseries']['password']


    try:
        response = requests.get(url, headers=headers, params=params, auth=(username, password))
        response.raise_for_status()  
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

    data = pd.json_normalize(response.json())
    data['event_at_utc'] = pd.to_datetime(data['event_at_utc'])
    data.set_index('event_at_utc', inplace=True)
    data.index = data.index.tz_localize('UTC')
    data.rename(columns={'value': name}, inplace=True)


    return data

def get_prices(start_datetime_utc, end_datetime_utc, country, auction):
    

    database = client['EpexSpot']    
    collection = database['AuctionPrices']   
    # build mongo db query for a date larger and lower than the start and end date  

    query = {"Country": country,
            "Auction": auction,}
    query["StartTimeUTC"] = {"$lt": end_datetime_utc, "$gte": start_datetime_utc}
    projection = {"_id": 0, 'StartTimeUTC':1, 'Auction': 1, 'Country': 1, 'DeliveryDay': 1, 'Value': 1}     
    forecast_document = list(collection.find(query, projection))

    spot_prices = pd.json_normalize(forecast_document)
    spot_prices.set_index('StartTimeUTC', inplace=True)

    return spot_prices


def get_ts_db(start_date, end_date, config, meta_id, name):


    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M")

    url = config['timeseries']['endpoint']
    headers = {'accept': 'application/json'}
    params = {'meta_data_id': meta_id, 'event_at_utc_from': start_date, 'event_at_utc_to': end_date}
    username = config['timeseries']['user']
    password = config['timeseries']['password']


    try:
        response = requests.get(url, headers=headers, params=params, auth=(username, password))
        response.raise_for_status()  
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

    data = pd.json_normalize(response.json())
    data['event_at_utc'] = pd.to_datetime(data['event_at_utc'])
    data.set_index('event_at_utc', inplace=True)
    data.index = data.index.tz_localize('UTC')
    data.rename(columns={'value': name}, inplace=True)


    return data

def get_ts_forecast(start_date, end_date, config, meta_id, name):


    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M")

    url = config['timeseries']['endpoint_forecast']
    headers = {'accept': 'application/json'}
    params = {'meta_data_id': meta_id, 'event_at_utc_from': start_date, 'event_at_utc_to': end_date}
    username = config['timeseries']['user']
    password = config['timeseries']['password']


    try:
        response = requests.get(url, headers=headers, params=params, auth=(username, password))
        response.raise_for_status()  
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

    data = pd.json_normalize(response.json())
    data['event_at_utc'] = pd.to_datetime(data['event_at_utc'])
    data.set_index('event_at_utc', inplace=True)
    data.index = data.index.tz_localize('UTC')
    data.rename(columns={'value': name}, inplace=True)


    return data

def get_ts_forecast_snapshot(start_date, end_date, pub_at_hour, config, data_key):


    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    date_range = pd.date_range(start_date, end_date)
    forecast = pd.DataFrame()

    for date_i in date_range:

        url = config['timeseries']['endpoint_forecast_snapshot']
        headers = {'accept': 'application/json'}
        params = {'data_key': data_key, 'pub_at_date': date_i.strftime("%Y-%m-%d"), 'pub_at_hour': pub_at_hour}
        username = config['timeseries']['user']
        password = config['timeseries']['password']


        try:
            response = requests.get(url, headers=headers, params=params, auth=(username, password))
            response.raise_for_status()  
            
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            

        data = pd.json_normalize(response.json())

        if data.iloc[0].values[0] == "No file/files found for the given parameters":
            
            print(f"Skipping data for date {date_i.strftime('%Y-%m-%d')} and hour {pub_at_hour} due to no files found.")
            new_hour = pub_at_hour - 6
            params_new = {'data_key': data_key, 'pub_at_date': date_i.strftime("%Y-%m-%d"), 'pub_at_hour': new_hour}
            response = requests.get(url, headers=headers, params=params_new, auth=(username, password))
            data = pd.json_normalize(response.json())

            if data.iloc[0].values[0] == "No file/files found for the given parameters":
                print(f"Skipping data for date {date_i.strftime('%Y-%m-%d')} and hour {pub_at_hour} due to no files found.")
                continue

        ## if empty go to the previous hour
        data['event_at_utc'] = pd.to_datetime(data['event_at_utc'])
        data.set_index('event_at_utc', inplace=True)
        data.index = data.index.tz_localize('UTC')
        data.rename(columns={'value': data_key}, inplace=True)
        forecast = pd.concat([forecast, data])

    forecast['published_at_utc'] = pd.to_datetime(forecast['published_at_utc']).dt.tz_localize('UTC')
    forecast['lead_time'] = forecast.index.tz_convert('Europe/Berlin').date - forecast['published_at_utc'].dt.tz_convert('Europe/Berlin').dt.date
    forecast = forecast.loc[forecast['lead_time'] == pd.Timedelta('1d')]
    forecast = forecast[data_key]

    return forecast

def get_capacity_data_for_each_key(start_date, end_date, config, key):

    key_list = config[key]
    start_date = start_date.strftime("%Y-%m-%dT%H:%M")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M")

    dataframes = []

    for key in key_list:

        url = config['timeseries']['endpoint']
        meta_id = key['id']
        headers = {'accept': 'application/json'}
        params = {'meta_data_id': meta_id, 'event_at_utc_from': start_date, 'event_at_utc_to': end_date}
        username = config['timeseries']['user']
        password = config['timeseries']['password']

        for _ in range(5):  # retry up to 5 times
            try:
                response = requests.get(url, headers=headers, params=params, auth=(username, password))
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f"Error: {e}")
                time.sleep(1)  # wait for 1 second before next retry
        else:
            print("Failed to retrieve data after 5 attempts. Moving to the next key.")
            continue

        cap = pd.json_normalize(response.json())
        cap['event_at_utc'] = pd.to_datetime(cap['event_at_utc'])
        cap.set_index('event_at_utc', inplace=True)
        cap.rename(columns={'value': key['data_key']}, inplace=True)

        dataframes.append(cap)

    combined_df = dataframes[0].join(dataframes[1:])

    return combined_df