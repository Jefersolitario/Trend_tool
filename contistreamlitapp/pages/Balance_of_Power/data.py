import requests
import time
from datetime import datetime, time
import certifi
from pymongo import MongoClient
import numpy as np
import pandas as pd
# from pages.Balance_of_Power.cross_border_trades import get_id_flow_xbid_evolution
from energyquantified import EnergyQuantified
from energyquantified.time import Frequency
from datetime import datetime, date, timedelta
import aiohttp
import asyncio

def mongo_client():
    return MongoClient(
            f"mongodb://app_power_dashboard:jQl6TZMfYe61Brbs@dev1-shard-00-00.uvhb7.mongodb.net:27017,dev1-shard-00-01.uvhb7.mongodb.net:27017,dev1-shard-00-02.uvhb7.mongodb.net:27017/test?authSource=admin&replicaSet=atlas-k6fhv2-shard-0&ssl=true",
            tz_aware=True,
            w="majority",
            readpreference="primary",
            journal=True,
            wTimeoutMS=600000,
            connect=False,
            tlsCAFile=certifi.where(),
            maxPoolSize=200,
        )


client = mongo_client()



def get_hydro(start, end, config):
    """
    API Access limited to 30 days
    """
    eq = EnergyQuantified(api_key=config['eq']['key'])
    start_date = start.tz_convert('Europe/Paris').date()
    end_date = end.tz_convert('Europe/Paris').date()


    
    hydro_latest = eq.instances.rolling(config['meta_data_id']['hydro'],
                                        start_date, end_date, hours_ahead =2,
                                        frequency= Frequency.PT30M)
    hydro_latest_df = hydro_latest.to_dataframe()

    day_ahead_forecast = eq.instances.relative(
    config['meta_data_id']['hydro'],
    begin= start_date,
    end= end_date,
    tag='',
    days_ahead=1,
    before_time_of_day=time(12, 0),  # Issued before 12 o'clock
    issued='latest',   # Set to "earliest" or "latest"
    frequency=Frequency.PT30M
    )
    hydro_dah = day_ahead_forecast.to_dataframe()
    df = hydro_dah.join(hydro_latest_df.add_suffix('_id'))
    df.columns = ['hydro dah', 'hydro ror id']

    return df


def get_nuclear_avail(start, end, config, id):

    eq = EnergyQuantified(api_key=config['eq']['key'])

    start_date = start.tz_convert('Europe/Paris').date()
    end_date = end.tz_convert('Europe/Paris').date()


    nuclear_avail_period = eq.period_instances.latest(id, begin= start_date, end= end_date)
    nuclear_avail_latest = nuclear_avail_period.to_dataframe(frequency=Frequency.PT30M)

    # timeseries_list.to_dataframe()
    day_ahead_forecast = eq.period_instances.relative(
    id,
    begin=start_date,
    end=end_date,
    days_ahead=1,
    before_time_of_day=time(12, 0),  # Issued before 12 o'clock
    )
    nuclear_avail_dah = day_ahead_forecast.to_dataframe(frequency=Frequency.PT30M)

    nuclear = nuclear_avail_latest.join(nuclear_avail_dah)
    latest_name = list(nuclear_avail_latest)[0][0] + list(nuclear_avail_latest)[0][1][:16]
    nuclear.columns = ['FR Nuclear Avail id', 'FR Nuclear Avail DAH'] #latest_name
    nuclear.metadata = {'Nuclear avail latest': latest_name}

    return nuclear

def get_nuclear_forecast(start, end, config, id):

    eq = EnergyQuantified(api_key=config['eq']['key'])

    start_date = start.tz_convert('Europe/Paris').date()
    end_date = end.tz_convert('Europe/Paris').date()

    day_ahead_forecast = eq.instances.relative(
    id,
    begin=start_date,
    end=end_date,
    tag = '',
    days_ahead=1,
    before_time_of_day=time(12, 0),  # Issued before 12 o'clock
    )
    nuclear_gen_dah = day_ahead_forecast.to_dataframe()
    nuclear_gen_dah_hh = nuclear_gen_dah.resample('30min').mean()
    nuclear_gen_dah_hh.columns = ['FR Nuclear gen forecast Day Ahead EQ']

    return nuclear_gen_dah_hh

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

def get_ts_hot_forecast(start_date, end_date, config, meta_id, name):


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

async def get_async_ts_hot_forecast(session, start_date, end_date, config, meta_id, name):
    # Format the dates
    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M")

    # Prepare the request parameters
    url = config['timeseries']['endpoint_forecast']
    headers = {'accept': 'application/json'}
    params = {'meta_data_id': meta_id, 'event_at_utc_from': start_date, 'event_at_utc_to': end_date}
    auth = aiohttp.BasicAuth(login=config['timeseries']['user'], password=config['timeseries']['password'])

    # Make an asynchronous HTTP request
    try:
        async with session.get(url, headers=headers, params=params, auth=auth) as response:
            response.raise_for_status()
            data = await response.json()
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

    # Process and return the data as a DataFrame
    df = pd.json_normalize(data)
    df['event_at_utc'] = pd.to_datetime(df['event_at_utc'])
    df.set_index('event_at_utc', inplace=True)
    df.index = df.index.tz_localize('UTC')
    df.rename(columns={'value': name}, inplace=True)

    return df

def get_ts_cold(start_date, end_date, config, data_key):


    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.tz_convert('Europe/Paris').strftime("%Y-%m-%d")
    end_date = end_date.tz_convert('Europe/Paris').strftime("%Y-%m-%d")

    date_range = pd.date_range(start_date, end_date)
    years = date_range.year.unique()
    months = date_range.month.unique()
    data = pd.DataFrame()


    url = config['timeseries']['endpoint_coldstorage']
    headers = {'accept': 'application/json'}
    username = config['timeseries']['user']
    password = config['timeseries']['password']


    for year in years:
        for month in months:
                
            params = {'data_key': data_key, 'event_at_year': year, 'event_at_month': month}

            try:
                response = requests.get(url, headers=headers, params=params, auth=(username, password))
                response.raise_for_status()  
                
            except requests.exceptions.RequestException as e:
                print(f"Error: {e}")
                

            data_i = pd.json_normalize(response.json())

            if 'detail' in data_i.columns:
                print(data_i['detail'][0])
                continue


            ## if empty go to the previous hour
            data_i['event_at_utc'] = pd.to_datetime(data_i['event_at_utc'])
            data_i.set_index('event_at_utc', inplace=True)
            data_i.index = data_i.index.tz_localize('UTC')
            data_i.rename(columns={'value': data_key}, inplace=True)
        
            data = pd.concat([data, data_i])
        
    return data

def get_ts_cold_snap(start_date, end_date, pub_at_hour, config, data_key):


    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    date_range = pd.date_range(start_date, end_date)
    forecast = pd.DataFrame()

    for date_i in date_range:

        # date_i = date_i - pd.Timedelta(days= 1)
        url = config['timeseries']['endpoint_coldstorage']
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

def get_old_ts_actuals(start_utc, end_utc, metadata_id):


    ids = metadata_id
    ids_list = list(ids.values())

    collection = client["TimeSeriesData"]["Actual"]
    query = {"MetaDataId": {"$in": ids_list}} 
    query["StartTimeUTC"] = {"$lt": end_utc, "$gte": start_utc}
    # query = {"MetaDataId":  "be1c2246-9fae-11ec-bfcd-a4bb6d5bfc5d"} 
    # query["StartTimeUTC"] = {"$lt": end_utc, "$gte": start_utc}

    # return specific fields
    projection = {"_id": 0,"MetaDataId": 1, "Value": 1, "StartTimeUTC": 1}

    records = list(collection.find(query, projection))
    # convert to dataframe
    df = pd.json_normalize(records)


    df["StartTimeUTC"] = pd.to_datetime(df["StartTimeUTC"])
    # df["StartTimeUTC"] = df["StartTimeUTC"].dt.tz_localize("UTC")

    data = df.pivot(index="StartTimeUTC", columns="MetaDataId", values="Value")
    data.reset_index(inplace=True)
    data.set_index('StartTimeUTC', inplace=True)
    # data.index.rename('datetime_cet', inplace=True)
    # data.index = data.index.tz_convert('Europe/Paris').tz_localize(None)
    # rename columns with ids
    # invert ids dictionary key value pairs
    ids_inv = {v: k for k, v in ids.items()}
    data.rename(columns=ids_inv, inplace=True)

    return data

def get_old_mongo_prices(start_datetime_utc, end_datetime_utc, country, auction):
    

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

def get_old_mongo_ts_feature(start_datetime_utc, end_datetime_utc, metadataid):
        
        
        db = client['TimeSeriesData']
        collection = db['FeatureStore']

        query = {"MetaDataId":metadataid}
        query["StartTimeUTC"] = {"$lte": end_datetime_utc, "$gte": start_datetime_utc}

        forecast = pd.DataFrame(list(collection.find(query)))

        forecast.set_index('StartTimeUTC', inplace=True)
        forecast = forecast.drop('MetaDataId', axis=1)
        forecast.rename(columns={'Value': metadataid}, inplace=True)

        return forecast[metadataid]

def _get_enappsys_data(enappsys, start, end):


    start = start.tz_convert('Europe/Paris').strftime('%Y%m%d%H%M')
    end = end.tz_convert('Europe/Paris').strftime('%Y%m%d%H%M')

    url = (
        enappsys['flow_endpoint']
        + "&start="
        + start
        + "&end="
        + end
    )
    data = pd.read_csv(url, index_col=0, parse_dates=True, dayfirst=True)
    data = data[1:]
    data.index = data.index.str.replace('[', '')
    data.index = data.index.str.replace(']', '')
    data.index = pd.to_datetime(data.index, dayfirst= True)
    data.index = data.index.tz_localize('Europe/Paris', ambiguous='infer')
    data.index = data.index.tz_convert('UTC')
    data.index = data.index.rename('Start Time UTC')

    return data

def get_gb_id_flows(start, end , config):

    flows = _get_enappsys_data(config['Enappsys'], start, end)

    id_flow_gb = flows['Great Britain Intraday']
    id_flow_gb = pd.to_numeric(id_flow_gb)

    return id_flow_gb

def get_id_flow_xbid_snap(start, end, country, duration, lead_time):
        """
        Get the latest flow available before lead time
        Fix bug on error: when no trades have happened because decoupling return null but it should take the previous flow
        available 
        """
        start = start - pd.Timedelta(hours= 2)
        start_str = start.tz_convert('Europe/Paris').strftime("%Y-%m-%d")
        end_str = end.tz_convert('Europe/Paris').strftime("%Y-%m-%d")

        id_flow_change = get_id_flow_xbid_evolution(start_str, end_str, country, duration)
        id_flow_change['lead_time'] = id_flow_change['Startdate CET'] - id_flow_change['ExecutionTimeCET']
        id_flow_change = id_flow_change.loc[id_flow_change['lead_time'] >= pd.Timedelta(minutes= lead_time)]
        id_flow_change = id_flow_change.drop_duplicates(subset=['Startdate CET'], keep= 'last') ## GIVE ME THE LATEST UM FLOW! by dropping duplicates 
        
        ## DO THE SAME FOR BACKTEST!!!
        id_flow_change.set_index('Startdate CET', inplace= True)
        id_flow_change.index = id_flow_change.index.tz_convert('utc')
        id_flow_change.rename({'AccumVolume': 'id flow change'}, axis=1, inplace= True)

        return id_flow_change

def get_energetech_id_flows(start, end,  id_flow_name, lead_time, config):

    country = 'FR'
    vwap_duration_window = str(5)  + "min"
    lead_time = 60
    start = start.tz_convert('utc')
    end = end.tz_convert('utc')
    id_flow = pd.DataFrame(index= pd.date_range(start, end, freq= 'h', tz = 'utc'))
    

    id_flow_gb = get_gb_id_flows(start, end,  config)
    # id_flow_change = _self.get_id_flow_xbid_snap_db(start, end, country, vwap_duration_window, lead_time)
    id_flow_change = get_id_flow_xbid_snap(start, end, country, vwap_duration_window, lead_time)
    
    list_flow = [id_flow_change['id flow change'], id_flow_gb]
    id_flow = id_flow.join(list_flow)
    ## Decouple Day Ahead up or down has NA volume on XBID filling with zero
    id_flow['id flow change'] = id_flow['id flow change'].fillna(0)
    ### enappsys has 3 days of missing data
    id_flow['Great Britain Intraday'] = id_flow['Great Britain Intraday'].fillna(0)
    # id_flow['Last Scheduled Flow'] = id_flow['DA Scheduled Flow'] + id_flow['id flow change'] + id_flow['Great Britain Intraday']
    id_flow['id Flow'] = id_flow['id flow change'] + id_flow['Great Britain Intraday']

    flow_id = id_flow['id Flow']

    flow = flow_id.reindex(pd.date_range(start=flow_id.index.min(), end=flow_id.index.max(), freq='30T'))
    flow = flow.fillna(method='ffill', limit= 1)

    return flow