import os
import certifi
import pandas as pd
import numpy as np
from pymongo import MongoClient
import requests


def get_environment() -> str:
    """Return current server environment."""
    environment = os.environ.get("ENVIRONMENT", "dev")
    return os.environ.get("SERVER_ENVIRONMENT", environment)

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
    spot_prices.index = spot_prices.index.tz_convert('Europe/Paris')
    spot_prices = spot_prices.rename(columns={'Value': 'price'})
    spot_prices.index.rename('datetime_cet', inplace=True)

    return spot_prices

def get_nordpool(start_datetime_utc, end_datetime_utc, country, auction):
    

    database = client['NordPool']    
    collection = database['AuctionResults']   
    # build mongo db query for a date larger and lower than the start and end date  

    query = {"Country": country,
            "Auction": auction,}
    query["StartTimeUTC"] = {"$lt": end_datetime_utc, "$gte": start_datetime_utc}
    projection = {"_id": 0, 'StartTimeUTC':1, 'Auction': 1, 'Country': 1, 'DeliveryDay': 1, 'Price': 1}     
    forecast_document = list(collection.find(query, projection))

    spot_prices = pd.json_normalize(forecast_document)
    spot_prices.set_index('StartTimeUTC', inplace=True)
    spot_prices.index = spot_prices.index.tz_convert('Europe/Paris')
    spot_prices = spot_prices.rename(columns={'Value': 'price'})
    spot_prices.index.rename('datetime_cet', inplace=True)

    return spot_prices

def get_actuals(start_utc, end_utc, metadata_id):


    ids = metadata_id
    ids_list = list(ids.values())

    collection = client["TimeSeriesData"]["Actual"]
    query = {"MetaDataId": {"$in": ids_list}} 
    query["StartTimeUTC"] = {"$lt": end_utc, "$gte": start_utc}

    # return specific fields
    projection = {"_id": 0,"MetaDataId": 1, "Value": 1, "StartTimeUTC": 1}

    records = list(collection.find(query, projection))
    # convert to dataframe
    df = pd.json_normalize(records)


    df["StartTimeUTC"] = pd.to_datetime(df["StartTimeUTC"])

    data = df.pivot(index="StartTimeUTC", columns="MetaDataId", values="Value")
    data.reset_index(inplace=True)
    data.set_index('StartTimeUTC', inplace=True)
    data.index.rename('datetime_cet', inplace=True)
    # rename columns with ids
    # invert ids dictionary key value pairs
    ids_inv = {v: k for k, v in ids.items()}
    data.rename(columns=ids_inv, inplace=True)

    return data


def get_vwap(country, product, start_utc, end_utc, lead_time, duration):
    """Get vwap data from MongoDB for a specific country and duration

    Args:
        country (str): country name
        duration (int): trading window duration in minutes
        start_utc (datetime): start datetime in UTC
        end_utc (datetime): end datetime in UTC
        lead_time (int): time to delivery in minutes

    """

    collection = client["EpexSpot"]['IndexPrices']
    product_map = {"XBID_Hour_Power": 60, "XBID_Quarter_Hour_Power": 15, "Intraday_Quarter_Hour_Power": 15,
                "2H Block vwap": 120, "4H Block vwap": 240, "HH vwap": 30}
    product_duration = product_map[product]
    

    if lead_time == None :
        query = {"DeliveryStartUTC": {"$lt": end_utc, "$gte": start_utc}, "Country":country, "Duration": product_duration,  "CandleDurationMinutes": duration}
    else: 
        lead_time_s = lead_time*60
        query = {"DeliveryStartUTC": {"$lt": end_utc, "$gte": start_utc}, "Country":country, "Duration": product_duration, "LeadTimeSeconds": lead_time_s, "CandleDurationMinutes": duration}

    vwap_index = pd.DataFrame(list(collection.find(query)))


    vwap_index["DeliveryStartUTC"] = pd.to_datetime(vwap_index["DeliveryStartUTC"])
    vwap_index["DeliveryStartUTC"] = vwap_index["DeliveryStartUTC"].dt.tz_convert("Europe/Paris")
    vwap_index.set_index("DeliveryStartUTC", inplace=True)
    # rename columns
    vwap_index.rename(columns={"VolumeMWh": "vwap volume"}, inplace=True)
    vwap_index.index.rename('datetime_cet', inplace=True)
    vwap_index.drop('_id', axis= 1, inplace= True)

    return vwap_index


def get_ts_db(start_date, end_date, config, key):



    start_str = start_date.strftime("%Y-%m-%dT%H:%M")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M")

    url = config['timeseries'][get_environment()]['endpoint']
    headers = {'accept': 'application/json'}
    params = {'meta_data_id': key, 'event_at_utc_from': start_str, 'event_at_utc_to': end_str}
    username = config['timeseries'][get_environment()]['user']
    password = config['timeseries'][get_environment()]['password']


    try:
        response = requests.get(url, headers=headers, params=params, auth=(username, password))
        response.raise_for_status()  
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        raise(e)

    data = pd.json_normalize(response.json())
    data['event_at_utc'] = pd.to_datetime(data['event_at_utc'])
    data = data.loc[data['event_at_utc'] < end_date.tz_localize(None)]
    data.set_index('event_at_utc', inplace=True)


    return data


def get_exaa_prices(start_datetime_utc, end_datetime_utc, country, auction):
    

    database = client['ENTSOE']    
    collection = database['DayAheadPrices']  
    # build mongo db query for a date larger and lower than the start and end date  

    query = {"MapCode": country,
            "ResolutionCode": auction,}
    query["DateTime"] = {"$lt": end_datetime_utc, "$gte": start_datetime_utc}
    projection = {"_id": 0, 'DateTime':1, 'MapCode': 1, 'Price': 1}     
    forecast_document = list(collection.find(query, projection))

    spot_prices = pd.json_normalize(forecast_document)
    spot_prices.set_index('DateTime', inplace=True)
    spot_prices.index = spot_prices.index.tz_convert('Europe/Paris')
    spot_prices = spot_prices.rename(columns={'Price': 'price'})
    spot_prices.index.rename('datetime_cet', inplace=True)

    return spot_prices
