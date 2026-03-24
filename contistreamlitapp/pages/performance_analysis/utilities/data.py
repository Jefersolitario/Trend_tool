import os
import certifi
import pandas as pd
from typing import Dict, List, Union
import pymongo
import numpy as np
from pymongo import MongoClient, UpdateOne, DESCENDING
import requests
# from contistreamlitapp.pages.performance_analysis.sheeze_session_init import build_seer_session
from pages.performance_analysis.sheeze_session_init import build_seer_session
import pytz
from datetime import datetime, timedelta
import logging
import streamlit as st

def get_environment() -> str:
    """Return current server environment."""
    environment = os.environ.get("ENVIRONMENT", "dev")
    return os.environ.get("SERVER_ENVIRONMENT", environment)

BERLIN_TIMEZONE = pytz.timezone("Europe/Berlin")
UTC_TIMEZONE = pytz.timezone("UTC")

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

def get_ts_db(start_date, end_date, config, meta_id, name):


    end_date = end_date + pd.Timedelta(minutes=45)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M")

    url = config['timeseries'][get_environment()]['endpoint']

    headers = {'accept': 'application/json'}
    params = {'meta_data_id': meta_id, 'event_at_utc_from': start_date, 'event_at_utc_to': end_date}
    username = config['timeseries'][get_environment()]['user']
    password = config['timeseries'][get_environment()]['password']


    try:
        response = requests.get(url, headers=headers, params=params, auth=(username, password))
        response.raise_for_status()  
        data = pd.json_normalize(response.json())
        data['event_at_utc'] = pd.to_datetime(data['event_at_utc'])
        data.set_index('event_at_utc', inplace=True)
        data.rename(columns={'value': name}, inplace=True)
        
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        data = pd.DataFrame(index= pd.date_range(start_date, end_date, freq= 'h'))
        data[name] = np.nan
        data.index.name = 'event_at_utc'   


    return data
    
def get_dah_positions(portfolio, product, country, start_utc, end_utc):

    collection = client["TradeData"]["TradeDeals"]

    record = list(
        collection.find(
            {"Product": product,
             "TradingPortfolio": {"$in":portfolio},
             "StartTimeUTC": {"$gte": start_utc, "$lte": end_utc}
            },
            sort=[("StartTimeUTC", pymongo.ASCENDING)]
        )
    )
    data = pd.json_normalize(record)

    if data.empty:
        print('DataFrame is empty!')
        return data
    
    data.loc[data["Side"] == "S", "VolumeMW"] = data["VolumeMW"] * -1
    data.loc[data['Side'] == 'S', 'buy'] = False
    data.loc[data['Side'] == 'B', 'buy'] = True

    data['StartTimeUTC'] = pd.to_datetime(data['StartTimeUTC'])
    data.set_index('StartTimeUTC', inplace=True)
    data.index = data.index.tz_convert("Europe/Paris")
    data.index.rename('StartTimeCET', inplace=True)
    
    data = data[data['Country'] == country]
    col = [ "Price", "VolumeMW"]
    data = data[col]
    data = agreggate_trading_portfolio(data)
    data = data.resample('30min').ffill(1) ##??????
    data = data.dropna()
    data = data._append(data.iloc[-1].rename(data.index[-1] + pd.Timedelta(minutes=30)))
    data.rename(columns={"Price": "DAH_Opening_Price", "VolumeMW": "DAH_Opening_Volume"}, inplace=True)
    data["DAH_Opening_Volume"] = data["DAH_Opening_Volume"].fillna(0)


    return data

def agreggate_trading_portfolio(data):

    aggregated_data = data.groupby(data.index).agg({'Price': 'first', 'VolumeMW': 'sum'})

    return aggregated_data

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
                "2H Block vwap": 120, "4H Block vwap": 240}
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

def get_own_trades(start_utc, end_utc, country, product, portfolio):


    collection = client["TradeData"]["TradeDeals"]
    query = {"Country": country,
            "Product": {"$in":product},
            "TradingPortfolio":  {"$in": portfolio},
            "StartTimeUTC": {"$gte": start_utc, "$lte": end_utc},
            "CommodityFamily": "POWER"}
         

    record = list(collection.find(query))

    data = pd.json_normalize(record)
    data['StartTimeUTC'] = pd.to_datetime(data['StartTimeUTC'])

    return data

def fetch_public_trades_data(
    products: Union[str, List[str]], 
    country_areas: Union[str, List[str]], 
    start: str, 
    end: str, 
    settings: Dict[str, any]
) -> pd.DataFrame:
    """
    Fetches trading data for specified products and country areas within a given time range.
    Accepts either single strings or lists of strings for products and country areas.

    Args:
        products (Union[str, List[str]]): Single product name or list of product names.
            Example values include "GB_Half_Hour_Power", "Intraday_Hour_Power", etc.
        
        country_areas (Union[str, List[str]]): Single delivery area code or list of codes.
            Example: "10YGB----------A" for Great Britain.
        
        start (str): The start timestamp for the data retrieval period in ISO 8601 format.
            Example: "2024-10-31T01:00:00Z"
        
        end (str): The end timestamp for the data retrieval period in ISO 8601 format.
            Example: "2024-10-31T12:00:00Z"

    Returns:
        pd.DataFrame: A pandas DataFrame containing the fetched trading data.
    
    Raises:
        TypeError: If products or country_areas are neither strings nor lists of strings.
    """
    # Convert single strings to lists for consistent processing
    if isinstance(products, str):
        products = [products]
    if isinstance(country_areas, str):
        country_areas = [country_areas]
    # Join the lists into comma-separated strings with space for proper encoding
    instruments = ', '.join(products)
    delivery_areas = ', '.join(country_areas)

    endpoint = settings['fetch_trade_api'][get_environment()]['trades_endpoint']
    # base_url = "https://portal-api.energetech.ae/series-updater/v2/algo-trading-data-fetcher/trades"
    
    # Define the query parameters
    params = {
        "instruments": instruments,
        "deliveryAreas": delivery_areas,
        "deliveryStartDateRangeUtc.startTs": start,
        "deliveryStartDateRangeUtc.endTs": end,
        "source": "EPEX"
    }
    
    headers = {'Accept': 'application/json'}
    
    try:
        response = requests.get(endpoint, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        df = pd.json_normalize(
            data['tradesToListedIsntruments'],
            record_path=['trades'],
            meta=[
                ['listedInstrument', 'product'],
                ['listedInstrument', 'market', 'area'],
                ['listedInstrument', 'market', 'country'],
                ['listedInstrument', 'deliveryDateRangeUtc', 'startTs'],
                ['listedInstrument', 'deliveryDateRangeUtc', 'endTs'],
                ['listedInstrument', 'tradingDateRangeUtc', 'startTs'],
                ['listedInstrument', 'tradingDateRangeUtc', 'endTs']
            ], sep= '_')

        df['timestampUtc'] = pd.to_datetime(df['timestampUtc'], format='ISO8601')
        df['listedInstrument_deliveryDateRangeUtc_startTs'] = pd.to_datetime(df['listedInstrument_deliveryDateRangeUtc_startTs'])
        
        return df
        
    except requests.exceptions.HTTPError as http_err:
        st.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        st.error(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        st.error(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        st.error(f"An error occurred: {req_err}")
        
    return pd.DataFrame()  # Return empty DataFrame if request fails

# get transactions data
def get_transactions(from_utc, to_utc, country, product):

    database = client["EpexSpot"]
    collection = database["IntradayTransactions"]

    query = {"$and": [{"DeliveryStartUTC": {"$gte": from_utc}},
                        {"DeliveryStartUTC": {"$lte": to_utc}},
                        {"Product": {"$in" :product}},
                        {"Country": country}]}

    fields = {"_id": 0,
                "TradeID": 1,
                "DeliveryStartUTC": 1,
                "DeliveryEndUTC": 1,
                "Price": 1,
                "VolumeMW": 1,
                "ExecutionTimeUTC": 1}

    transactions = pd.DataFrame(list(collection.find(query, fields)))

    if transactions.empty:
        return transactions

    transactions.drop_duplicates(inplace=True)
    transactions["StartTimeUTC"] = transactions['DeliveryStartUTC']
    transactions["EndTimeUTC"] = transactions['DeliveryEndUTC']
    transactions["ExecutionTimeUTC"] = transactions['ExecutionTimeUTC']
    return transactions

def calc_vol_based_vwap(country, product, from_utc, to_utc, vol): 
    """
    Refactor with proper labels 
    """
    try:
        intraday_trades = get_transactions(from_utc, to_utc, country, [product])
        
        intraday_trades['cum_vol'] = intraday_trades.groupby('StartTimeUTC')['VolumeMW'].cumsum()
        intraday_trades = intraday_trades[intraday_trades['cum_vol'] <= vol]

        intraday_trades['trade_value'] = intraday_trades['Price']*intraday_trades['VolumeMW']

        groups_wap = intraday_trades.groupby(['DeliveryStartUTC'])
        # trade_n_min = groups_wap.sum()[['trade_value','VolumeMW']]
        trade_n_min = groups_wap[['trade_value', 'VolumeMW']].sum()

        trade_n_min['vwap'] = trade_n_min['trade_value']/trade_n_min['VolumeMW']

        trade_n_min.drop('trade_value', axis = 1, inplace = True)

        trade_n_min['Country'] = country
        trade_n_min['Product'] = product
        trade_n_min['Volume'] = vol
        trade_n_min.index.name = 'StartTimeUTC'

        trade_n_min.index = trade_n_min.index.tz_convert('Europe/Paris')
        trade_n_min.index.rename('datetime_cet', inplace=True)


    except:
        date_range = pd.date_range(from_utc, to_utc, freq='60min', tz='utc')
        trade_n_min = pd.DataFrame(index=date_range, columns=['VolumeMW_'+country, 'vwap_'+country])



    return trade_n_min

def save_to_db(vwaps):

    collection = client['testDB']['vwaps_vol_intervals']

    data_dict = vwaps.to_dict(orient='records')
    # collection.insert_many(data_dict)
        # Preparing bulk operations
    operations = []
    for record in data_dict:
        query = {
            'StartTimeUTC': record['StartTimeUTC'],
            'Country': record['Country'],
            'Product': record['Product'],
            'Volume': record['Volume'],
            'VolumeMW': record['VolumeMW']
        }
        operations.append(UpdateOne(query, {'$set': record}, upsert=True))

    # Execute bulk operations
    if operations:
        collection.bulk_write(operations, ordered=False)

def update_vwap_vol(country, product, from_utc, to_utc, vol):

    collection = client['testDB']['vwaps_vol_intervals']
    query = {'Country': country, 'Product': product, 'Volume': vol}

    most_recent_doc = collection.find(query).sort("StartTimeUTC", DESCENDING).limit(1)
    most_recent_df  = pd.DataFrame(list(most_recent_doc))

    if most_recent_df.empty == True:
        print('No data found creating the vwap')
        vwap_update = calc_vol_based_vwap(country, product, from_utc, to_utc, vol)
        vwap_update.index = vwap_update.index.tz_convert('utc')
        vwap_update.index.name = 'StartTimeUTC'
        vwap_update.reset_index(inplace=True)
        save_to_db(vwap_update)
        print('saved to db')
        return


    if most_recent_df['StartTimeUTC'][0] < to_utc:
        new_start_utc = most_recent_df['StartTimeUTC'][0]
        new_start_utc = new_start_utc.tz_convert('utc')
        vwap_update = calc_vol_based_vwap(country, product, new_start_utc, to_utc, vol)
        ## No new trades on db exit
        if vwap_update.dropna().empty == True:
            return
        vwap_update.index = vwap_update.index.tz_convert('utc')
        vwap_update.index.name = 'StartTimeUTC'
        vwap_update.reset_index(inplace=True)
        save_to_db(vwap_update)
        print('saved to db')
        return
    else:
        print('nothing new')
        return


    return 

def get_vol_based_vwap(country, product, from_utc, to_utc, vol):

    # get vol vwap last date

    update_vwap_vol(country, product, from_utc, to_utc, vol)

    collection = client['testDB']['vwaps_vol_intervals']
    query = {'Country': country, 'Product': product, 'StartTimeUTC': {'$gte': from_utc, '$lte': to_utc}, 'Volume': vol}

    tradevwap = pd.DataFrame(list(collection.find(query)))
    tradevwap = tradevwap.drop('_id', axis=1)
    tradevwap.set_index('StartTimeUTC', inplace= True)


    return tradevwap

# vol_tranches = list(range(1000,3500,500))
# to_utc = pd.to_datetime('today').floor('d').tz_localize('Europe/Paris').tz_convert('utc')
# from_utc = to_utc - pd.Timedelta(days= 365)
# for vol in vol_tranches:
#     update_vwap_vol('NL', 'XBID_Hour_Power', from_utc, to_utc, vol)
# print('done')

def get_intraday_positions(country, product, start_utc, end_utc, positions_endpoint):

    intraday_positions_l = pd.DataFrame()
    dates = pd.date_range(start_utc, end_utc, freq='D', tz='utc')
    dates = dates.tz_convert('Europe/Paris')

    params = {'date': end_utc,
        'country': country,
        'timeResolution': '60'}
    headers = {
    'accept': 'application/json'}

    for date in dates:
        
        params['date'] = date.strftime("%Y-%m-%d")
        response = requests.get(positions_endpoint, headers=headers, params=params)

        if response.status_code == 200:

            data = pd.DataFrame(response.json())
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")

        intraday_positions_l =intraday_positions_l._append(data)

    return intraday_positions_l

def get_total_pnl(country, portfolio, start_utc, end_utc, resolution, settings):

    endpoint = settings['Total PnL'][get_environment()]['pnl_endpoint']
    trading_group = settings['Total PnL']['porfolio_group']

    with build_seer_session() as s:
        params = {
            "country" :country,
            'aggKeys': trading_group,
            "commodityFamily": "POWER",
            "portfolio": portfolio,
            "startDateTime": start_utc.tz_convert('Europe/Paris').strftime("%Y-%m-%d"),
            "endDateTime": end_utc.tz_convert('Europe/Paris').strftime("%Y-%m-%d"),
            "agg": resolution,

        }
        reply = s.get(endpoint, params=params)
        reply.raise_for_status()

        position_pnl = pd.DataFrame(reply.json())
        position_pnl['StartTime'] = pd.to_datetime(position_pnl['StartTime'])
        position_pnl.set_index('StartTime', inplace= True)

    return position_pnl


def get_nominated_positions(start: datetime, end: datetime, country: str, horizon: str, app_config: dict):

    cable = app_config['flow_nomination']['cables'][country]
    start_time_cet = BERLIN_TIMEZONE.localize(start)
    start_time_utc = start_time_cet.astimezone(UTC_TIMEZONE)
    end_time_cet = BERLIN_TIMEZONE.localize(end)
    end_time_utc = end_time_cet.astimezone(UTC_TIMEZONE)

    df = pd.DataFrame(
        pd.date_range(start=start_time_utc, end= end_time_utc, freq="H"),
        columns=["StartTimeUTC"],
    )

    countries = app_config["flow_nomination"]["CrossBorderCables"].get(cable, None)

    df[f"{countries['Country1'] + countries['Country2']}Nom"] = 0
    df[f"{countries['Country2'] + countries['Country1']}Nom"] = 0
    df["cable"] = cable

    df_flow_trades = pd.DataFrame(get_flow_trades(start_time_utc, end_time_utc, cable, horizon, app_config))

    for index, row in df_flow_trades.iterrows():
        volume_field = "NominatedVolume" if "NominatedVolume" in row else "VolumeMW"
        df.loc[
            df["StartTimeUTC"] == row["StartTimeUTC"],
            f"{row['CountryFrom']}{row['CountryTo']}Nom",
        ] = row[volume_field]

    return df


def get_flow_trades(start_time_utc: datetime, end_time_utc: datetime, cable: str, horizon: str, settings: dict):

    return list(
        client[settings["flow_nomination"]["db"]][settings["flow_nomination"]["collection"]].find(
            {
                "StartTimeUTC": {
                    "$gte": start_time_utc,
                    "$lt": end_time_utc + timedelta(1),
                },
                "Side": "S",
                "TradeType": "FLOW",
                "Product": horizon,
                "Cable": cable,
            },
            {
                "_id": False,
                "StartTimeUTC": 1,
                "EndTimeUTC": 1,
                "VolumeMW": 1,
                "CountryFrom": 1,
                "CountryTo": 1,
                "Product": 1,
                "NominatedVolume": 1,
                "Cable": 1
            },
        )
    )


def get_ladder_enappsys(start, end, enappsys):

    start = start.tz_convert('Europe/Paris').strftime('%Y%m%d%H%M')
    end = end + pd.Timedelta(hours= 1)
    end = end.tz_convert('Europe/Paris').strftime('%Y%m%d%H%M')

    url = f"{enappsys['endpoint']}&start={start}&end={end}"
    ladder_col = ['NEGATIVE PRICE (600)', 'NEGATIVE PRICE (300)', 'NEGATIVE PRICE (100)', 'POSITIVE PRICE (100)', 'POSITIVE PRICE (300)', 'POSITIVE PRICE (600)']

    try:
        data = pd.read_csv(url, index_col=0, parse_dates=True, dayfirst=True)
    except Exception as e:
        logging.error(f"An error occurred while fetching the data: {e}")
        data = pd.DataFrame(index= pd.date_range(start, end, tz= 'utc'))
        data[ladder_col] = np.nan
        return data
    if data.empty:
        logging.info("The fetched data is empty.")
        data = pd.DataFrame(index= pd.date_range(start, end, tz= 'utc'))
        data[ladder_col] = np.nan
        return data
    
    data = data[1:]
    data.index = data.index.str.replace('[', '')
    data.index = data.index.str.replace(']', '')
    data.index = pd.to_datetime(data.index, dayfirst= True)
    data.index = data.index.tz_localize('Europe/Paris', ambiguous='infer')
    data.index = data.index.tz_convert('UTC')
    data.index = data.index.rename('Start Time UTC')

    return data

def get_niv_enappsys(start, end, enappsys):

    start = start.tz_convert('Europe/Paris').strftime('%Y%m%d%H%M')
    end = end + pd.Timedelta(hours= 1)
    end = end.tz_convert('Europe/Paris').strftime('%Y%m%d%H%M')

    url = f"{enappsys['endpoint_niv']}&start={start}&end={end}"
    niv_col = ['aFRR UP', 'aFRR DOWN', "IGCC UP", "IGCC DOWN"]

    try:
        data = pd.read_csv(url, index_col=0, parse_dates=True, dayfirst=True)
    except Exception as e:
        logging.error(f"An error occurred while fetching the data: {e}")
        data = pd.DataFrame(index= pd.date_range(start, end, tz= 'utc'))
        niv_col = ["UPWARD DISPATCH", "DOWNWARD DISPATCH", "IGCC UP", "IGCC DOWN"]
        data[niv_col] = np.nan
        return data
    if data.empty:
        logging.info("The fetched data is empty.")
        data = pd.DataFrame(index= pd.date_range(start, end, tz= 'utc'))
        data[niv_col] = np.nan
        return data
    
    data = data[1:]
    data.index = data.index.str.replace('[', '')
    data.index = data.index.str.replace(']', '')
    data.index = pd.to_datetime(data.index, dayfirst= True)
    data.index = data.index.tz_localize('Europe/Paris', ambiguous='infer')
    data.index = data.index.tz_convert('UTC')
    data.index = data.index.rename('Start Time UTC')

    data = data.apply(pd.to_numeric)

    return data

def get_midprice_enappsys(start, end, enappsys):

    start = start.tz_convert('Europe/Paris').strftime('%Y%m%d%H%M')
    end = end + pd.Timedelta(hours= 1)
    end = end.tz_convert('Europe/Paris').strftime('%Y%m%d%H%M')


    url = f"{enappsys['mid_price']}&start={start}&end={end}"
    
    try:
        data = pd.read_csv(url, index_col=0, parse_dates=True, dayfirst=True)
    except Exception as e:
        logging.error(f"An error occurred while fetching the data: {e}")
        data = pd.DataFrame(index= pd.date_range(start= start, end= end, tz= 'utc'))
        data['MID PRCE'] = np.nan
        return data
    
    if data.empty:
        logging.info("The fetched data is empty.")
        data = pd.DataFrame(index= pd.date_range(start, end, tz= 'utc'))
        data['MID PRCE'] = np.nan
        return data

    data = data[1:]
    data.index = data.index.str.replace('[', '')
    data.index = data.index.str.replace(']', '')
    data.index = pd.to_datetime(data.index, dayfirst= True)
    data.index = data.index.tz_localize('Europe/Paris', ambiguous='infer')
    data.index = data.index.tz_convert('UTC')
    data.index = data.index.rename('Start Time UTC')

    data = data.apply(pd.to_numeric)

    return data


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
    forecast_document = list(collection.find(query)) #, projection
    
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

