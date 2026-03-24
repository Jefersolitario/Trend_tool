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

def get_actuals(start_utc, end_utc, metadata_id, name):



    collection = client["TimeSeriesData"]["Actual"]
    query = {"MetaDataId": metadata_id} 
    query["StartTimeUTC"] = {"$lt": end_utc, "$gte": start_utc}

    # return specific fields
    projection = {"_id": 0,"MetaDataId": 1, "Value": 1, "StartTimeUTC": 1}

    records = list(collection.find(query, projection))
    # convert to dataframe
    data = pd.json_normalize(records)


    data["StartTimeUTC"] = pd.to_datetime(data["StartTimeUTC"])
    data.set_index('StartTimeUTC', inplace=True)
    data = data.rename(columns={'Value': name})
    data = data[name]

    return data

def get_spot_prices(start_datetime_utc, end_datetime_utc, auction, name):
    

    database = client['EpexSpot']    
    collection = database['AuctionPrices']   
    # build mongo db query for a date larger and lower than the start and end date  

    query = {"Auction": auction}
    query["StartTimeUTC"] = {"$lt": end_datetime_utc, "$gte": start_datetime_utc}
    projection = {"_id": 0, 'StartTimeUTC':1, 'Auction': 1, 'Country': 1, 'DeliveryDay': 1, 'Value': 1}     
    forecast_document = list(collection.find(query, projection))

    spot_prices = pd.json_normalize(forecast_document)
    spot_prices.set_index('StartTimeUTC', inplace=True)
    spot_prices.index = spot_prices.index.tz_convert('Europe/Paris')
    spot_prices = spot_prices.rename(columns={'Value': name})
    spot_prices.index.rename('datetime_cet', inplace=True)

    return spot_prices

def get_vwap_index(country, product, delivery_start, delivery_end, lead_time, duration):

        collection = client['EpexSpot']['IndexPrices']
        ## indetify production according to duration
        product_map = {"XBID_Hour_Power": 60, "XBID_Half_Hour_Power": 30,  "Intraday_Half_Hour_Power": 30,
                       "XBID_Quarter_Hour_Power": 15, "Intraday_Quarter_Hour_Power": 15, "2H Block vwap": 120, "4H Block vwap": 240, "HH vwap": 30}
        product_duration = product_map[product]
        freq_str = f'{product_duration}min'

        lead_time_sec = lead_time*60

        query = {"DeliveryStartUTC": {"$lt": delivery_end, "$gt": delivery_start}, "Country":country, "Duration": product_duration, "LeadTimeSeconds": lead_time_sec, "CandleDurationMinutes": duration}

        vwap_index = pd.DataFrame(list(collection.find(query)))

        vwap_index.set_index('DeliveryStartUTC', inplace=True)
        vwap_index.drop("_id", axis= 1 , inplace= True)

        df = pd.DataFrame(index = pd.date_range(start=delivery_start, end= delivery_end, freq= freq_str))
        # df.index = df.index.tz_localize('utc')
        df = df.join(vwap_index)

        return df

def get_strategy(start_datetime_utc, end_datetime_utc, strategy):

    database = client['Conti']    
    collection = database['SignalMetaData']

    query_id = {'name': strategy}
    strategy_metadata = list(collection.find(query_id))
    id = str(strategy_metadata[0]['_id'])

    collection_signal = database['Signals']
    query = {"MetaKey": id, "StartTimeUTC": {"$lt": end_datetime_utc, "$gte": start_datetime_utc}}
    forecast_document = list(collection_signal.find(query))
    signal = pd.json_normalize(forecast_document)
    signal.columns = [col.replace('feature_data.', '') for col in signal.columns]
    signal.drop('_id', axis= 1, inplace= True)


    signal['StartTimeUTC'] = pd.to_datetime(signal['StartTimeUTC'], utc= True)
    signal['CalculationTimeUTC'] = pd.to_datetime(signal['CalculationTimeUTC'], utc= True)
    signal['lead_time'] = signal['StartTimeUTC'] - signal['CalculationTimeUTC']
    signal.sort_values(by = ['StartTimeUTC', 'CalculationTimeUTC'], inplace= True)
    signal.drop_duplicates(subset = 'StartTimeUTC', keep= 'last', inplace= True)

    signal.set_index('StartTimeUTC', inplace=True)

    return signal

def get_strategy_old(start_datetime_utc, end_datetime_utc, strategy):
    """
    the signal must have at least the following collections:
    StartTimeUTC
    ClculationTimeUTC
    Data (contains the signal, StartTimeUTC)
    """

    database = client['testDB']    
    collection = database[strategy]   

    query = {"DeliveryDayCET": {"$lt": end_datetime_utc, "$gte": start_datetime_utc}}   
    forecast_document = list(collection.find(query))

    if len(forecast_document) == 0:
        signal = None
        return signal

    sample_doc = forecast_document[0]
    meta_cols = [key for key in sample_doc.keys() if key != 'Data']
    signal = pd.json_normalize(forecast_document, record_path= 'Data', meta = meta_cols, errors = 'ignore')
    signal.drop('_id', axis= 1, inplace= True)

    signal['StartTimeUTC'] = pd.to_datetime(signal['StartTimeUTC'], utc= True)
    signal['CalculationTimeUTC'] = pd.to_datetime(signal['CalculationTimeUTC'], utc= True)
    signal['lead_time'] = signal['StartTimeUTC'] - signal['CalculationTimeUTC']
    signal.sort_values(by = ['StartTimeUTC', 'CalculationTimeUTC'], inplace= True)
    signal.drop_duplicates(subset = 'StartTimeUTC', keep= 'last', inplace= True)

    signal.set_index('StartTimeUTC', inplace=True)

    return signal

def get_ts_db(start_date, end_date, config, key, name):



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
    data.rename(columns={'value': name}, inplace=True)
    data.index = data.index.tz_localize('utc').tz_convert('Europe/Paris')

    return data

def get_ts_history(start_date, end_date, config, data_key, name):


    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    date_range = pd.date_range(start_date, end_date)
    years = date_range.year.unique()
    data_hist = pd.DataFrame()


    for year_i in years:

        url = config['timeseries']['endpoint_history']
        headers = {'accept': 'application/json'}
        params = {'data_key': data_key, 'event_at_year': year_i} #, 'event_at_month': pub_at_hour
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
        data.index = data.index.tz_localize('UTC').tz_convert('Europe/Paris')
        data.rename(columns={'value': name}, inplace=True)
        data_hist = pd.concat([data_hist, data])

    data_hist = data_hist[name]

    return data_hist


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
        query = {"DeliveryStartUTC": {"$lt": end_utc, "$gt": start_utc}, "Country":country, "Duration": product_duration,  "CandleDurationMinutes": duration}
    else: 
        lead_time_s = lead_time*60
        query = {"DeliveryStartUTC": {"$lt": end_utc, "$gt": start_utc}, "Country":country, "Duration": product_duration, "LeadTimeSeconds": lead_time_s, "CandleDurationMinutes": duration}

    vwap_index = pd.DataFrame(list(collection.find(query)))


    vwap_index["DeliveryStartUTC"] = pd.to_datetime(vwap_index["DeliveryStartUTC"])
    vwap_index["DeliveryStartUTC"] = vwap_index["DeliveryStartUTC"].dt.tz_convert("Europe/Paris")
    vwap_index.set_index("DeliveryStartUTC", inplace=True)
    # rename columns
    vwap_index.rename(columns={"VolumeMWh": "vwap volume"}, inplace=True)
    vwap_index.index.rename('datetime_cet', inplace=True)
    vwap_index.drop('_id', axis= 1, inplace= True)

    return vwap_index

