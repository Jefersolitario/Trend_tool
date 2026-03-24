import os
import time
import certifi
from pymongo import MongoClient
import pandas as pd
import requests
import streamlit as st

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

def get_vwap_index(country, product, delivery_start, delivery_end, lead_time, duration):
    collection = client['EpexSpot']['IndexPrices']
    product_map = {"XBID_Hour_Power": 60, "XBID_Half_Hour_Power": 30,  "Intraday_Half_Hour_Power": 30,
                   "XBID_Quarter_Hour_Power": 15, "Intraday_Quarter_Hour_Power": 15, "2H Block vwap": 120, "4H Block vwap": 240, "HH vwap": 30}
    product_duration = product_map[product]
    freq_str = f'{product_duration}min'

    lead_time_sec = lead_time * 60

    # query = {"DeliveryStartUTC": {"$lt": delivery_end, "$gt": delivery_start}, "Country": country, "Duration": product_duration, "LeadTimeSeconds": lead_time_sec, "CandleDurationMinutes": duration}
    # start = time.time()
    print(lead_time_sec, country, product_duration, duration)
    query = {"DeliveryStartUTC": {"$lt": delivery_end, "$gt": delivery_start}, "LeadTimeSeconds": lead_time_sec, "Country": country, "Duration": product_duration, "CandleDurationMinutes": duration}
    vwap_index = pd.DataFrame(list(collection.find(query)))
    # print(time.time() - start)

    vwap_index.set_index('DeliveryStartUTC', inplace=True)
    vwap_index.drop("_id", axis=1, inplace=True)

    df = pd.DataFrame(index=pd.date_range(start=delivery_start, end=delivery_end, freq=freq_str))
    df = df.join(vwap_index)

    return df

def get_strategy(start_datetime_utc, end_datetime_utc, id):
    database = client['Conti']    
    collection_signal = database['Signals']
    query = {"MetaKey": id, "StartTimeUTC": {"$lt": end_datetime_utc, "$gte": start_datetime_utc}}
    forecast_document = list(collection_signal.find(query))
    signal = pd.json_normalize(forecast_document)
    signal.columns = [col.replace('feature_data.', '') for col in signal.columns]
    signal.drop('_id', axis=1, inplace=True)

    signal['StartTimeUTC'] = pd.to_datetime(signal['StartTimeUTC'], utc=True)
    signal['CalculationTimeUTC'] = pd.to_datetime(signal['CalculationTimeUTC'], utc=True)
    signal['lead_time'] = signal['StartTimeUTC'] - signal['CalculationTimeUTC']
    signal.sort_values(by=['StartTimeUTC', 'CalculationTimeUTC'], inplace=True)
    signal.drop_duplicates(subset='StartTimeUTC', keep='last', inplace=True)

    signal.set_index('StartTimeUTC', inplace=True)

    return signal

def get_ts_db(start_date, end_date, config, key, name):
    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M")

    # url = config['timeseries']['endpoint']
    url = config['timeseries'][get_environment()]['endpoint']
    # url = 'https://portal-api.energetech.ae/timeseriesrestapi/hotstorage/latest/actuals/?'


    headers = {'accept': 'application/json'}
    params = {'meta_data_id': key, 'event_at_utc_from': start_date, 'event_at_utc_to': end_date}
    # username = config['timeseries']['user']
    # password = config['timeseries']['password']
    username = config['timeseries'][get_environment()]['user']
    password = config['timeseries'][get_environment()]['password']

    try:
        response = requests.get(url, headers=headers, params=params, auth=(username, password))
        response.raise_for_status()  
    except requests.exceptions.RequestException as e:
        st.error(f"An error occurred while fetching data: {e}")
        print(f"Error: {e}")

    data = pd.json_normalize(response.json())
    data['event_at_utc'] = pd.to_datetime(data['event_at_utc'])
    data.set_index('event_at_utc', inplace=True)
    data.rename(columns={'value': name}, inplace=True)
    data.index = data.index.tz_localize('utc').tz_convert('Europe/Paris')

    return data

def get_vwap(country, product, start_utc, end_utc, lead_time, duration):
    collection = client["EpexSpot"]['IndexPrices']
    product_map = {"XBID_Hour_Power": 60, "XBID_Quarter_Hour_Power": 15, "Intraday_Quarter_Hour_Power": 15,
                "2H Block vwap": 120, "4H Block vwap": 240, "HH vwap": 30}
    product_duration = product_map[product]

    if lead_time is None:
        query = {"DeliveryStartUTC": {"$lt": end_utc, "$gt": start_utc}, "Country": country, "Duration": product_duration,  "CandleDurationMinutes": duration}
    else: 
        lead_time_s = lead_time * 60
        query = {"DeliveryStartUTC": {"$lt": end_utc, "$gt": start_utc}, "Country": country, "Duration": product_duration, "LeadTimeSeconds": lead_time_s, "CandleDurationMinutes": duration}

    vwap_index = pd.DataFrame(list(collection.find(query)))

    vwap_index["DeliveryStartUTC"] = pd.to_datetime(vwap_index["DeliveryStartUTC"])
    vwap_index["DeliveryStartUTC"] = vwap_index["DeliveryStartUTC"].dt.tz_convert("Europe/Paris")
    vwap_index.set_index("DeliveryStartUTC", inplace=True)
    vwap_index.rename(columns={"VolumeMWh": "vwap volume"}, inplace=True)
    vwap_index.index.rename('datetime_cet', inplace=True)
    vwap_index.drop('_id', axis=1, inplace=True)

    return vwap_index