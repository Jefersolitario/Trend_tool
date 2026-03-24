import os

import certifi
import yaml
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.subplots as sp
import streamlit as st
from pymongo import MongoClient, DESCENDING
import requests

import plotly.graph_objects as go
from scipy import stats
from pages.BSADs_flows.data import *
# from contistreamlitapp.pages.BSADs_flows.data import *

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


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


def auction_requirements(auction_id, settings):


    url = settings['auction_requirements'][get_environment()]['endpoint']
    headers = {'accept': 'application/json'}
    params = {'auctionId': auction_id}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

    meta_col = ['id', 'dateStarted', 'biddingCloseDate', 'status', 'dateCompleted', 'dateCancelled', 'cancellationReason', 'currentVersionNumber']
    requirements_details = pd.json_normalize(response.json(), record_path ='lots', meta= meta_col)

    return requirements_details

def get_bsads_requirements(start, end, settings):
    """
    edge case: add 1 more extra day to start date to ensure all requirements are captured
    for the next day
    """
    start_plus = start - pd.Timedelta(days=1)
    start_plus = pd.to_datetime(start_plus).strftime('%Y-%m-%dT%H:%M:%SZ')
    end = pd.to_datetime(end).strftime('%Y-%m-%dT%H:%M:%SZ')

    url = settings[get_environment()]['requirements_endpoint']
    headers = {'accept': 'application/json'}
    params = {'startedAfter': start_plus, 'startedBefore': end, 'page': 1, 'perPage': 200}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    total_pages = response.json()['pages']['max']
    all_requirements = []

    # Loop through all pages to fetch data
    for page in range(1, total_pages + 1):
        params['page'] = page
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page_data = response.json()['results']
        all_requirements.extend(page_data)

    # Convert to DataFrame and process
    bsads_requirements = pd.DataFrame(all_requirements)
    # bsads_requirements = pd.DataFrame(response.json()['results'])

    bsads_requirements['dateStarted'] = pd.to_datetime(bsads_requirements['dateStarted'])
    bsads_requirements.rename(columns={'dateStarted': 'publicationTime'}, inplace=True)

    requirements_details = bsads_requirements['id'].apply(lambda x: auction_requirements(x, settings)).tolist()
    requirements_details = pd.concat(requirements_details, ignore_index=True)


    requirements_details = requirements_details.loc[pd.to_datetime(requirements_details['start']) >= start.tz_localize('UTC')]
    # multiple_requirements = requirements_details.loc[(requirements_details['start'].duplicated() == True) & (requirements_details['status'] == 'Complete')]

    return requirements_details

def get_transactions(from_utc, to_utc, country, product):

    database = client["EpexSpot"]
    collection = database["IntradayTransactions"]

    query = {"$and": [{"DeliveryStartUTC": {"$gte": from_utc}},
                        {"DeliveryStartUTC": {"$lte": to_utc}},
                        {"Product": product},
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
    transactions["StartTimeUTC"] = transactions['DeliveryStartUTC'].dt.tz_localize("UTC")
    transactions["EndTimeUTC"] = transactions['DeliveryEndUTC'].dt.tz_localize("UTC")
    transactions["ExecutionTimeUTC"] = transactions['ExecutionTimeUTC'].dt.tz_localize("UTC")
    return transactions

def calc_vwap_gc(from_utc, to_utc, country, product, lead_time ,duration=34*60):
    """
    Refactor with proper labels
    lead_time in minutes
    duration in minutes
    """

    intraday_trades = get_transactions(from_utc, to_utc, country, product)
    intraday_trades['lead_time'] = intraday_trades['StartTimeUTC'] - intraday_trades['ExecutionTimeUTC']
    intraday_trades = intraday_trades[intraday_trades['lead_time'] >= pd.Timedelta(minutes =lead_time)]
    intraday_trades = intraday_trades[intraday_trades['lead_time'] <= pd.Timedelta(minutes =(lead_time + duration))]

    intraday_trades['trade_value'] = intraday_trades['Price']*intraday_trades['VolumeMW']

    groups_wap = intraday_trades.groupby(['DeliveryStartUTC'])
    trade_n_min = groups_wap.sum()[['trade_value','VolumeMW']]

    trade_n_min['vwap'] = trade_n_min['trade_value']/trade_n_min['VolumeMW']
    trade_n_min = trade_n_min.reset_index()

    # convert DeliverStartUTC to CET set as an index and add country suffx with Country
    trade_n_min = trade_n_min.set_index('DeliveryStartUTC')
    trade_n_min.index = trade_n_min.index.tz_localize('UTC').tz_convert('Europe/London')
    trade_n_min.index.rename('datetime_cet', inplace=True)
    trade_n_min = trade_n_min.add_suffix('_'+country)
    trade_n_min.drop(columns=['trade_value_'+country], inplace=True)


    return trade_n_min


def filter_trades(intraday_transactions, requirements, duration):

    ## filter requirement time by  transanction time + delta
    requirements_filter = requirements.copy()
    requirements_filter.rename(columns={'start': 'DeliveryStartUTC', 'dateStarted':'TradeStartUTC'}, inplace=True)
    # join by delivery start time
    ## think about start duplicates !!!!!!!
    requirements_filter['TradeStartUTC'] = pd.to_datetime(requirements_filter['TradeStartUTC'])
    requirements_filter['TradeEndUTC'] = requirements_filter['TradeStartUTC'] + pd.Timedelta(minutes=duration)
    col_filter = ['DeliveryStartUTC', 'TradeStartUTC', 'TradeEndUTC']
    requirements_filter = requirements_filter[col_filter]

    merged_df = pd.merge(requirements_filter, intraday_transactions, on='DeliveryStartUTC', how='inner')

    # Convert the relevant columns to datetime format (if they are not already)
    merged_df['TradeStartUTC'] = pd.to_datetime(merged_df['TradeStartUTC'])
    merged_df['TradeEndUTC'] = pd.to_datetime(merged_df['TradeEndUTC'])
    merged_df['ExecutionTimeUTC'] = pd.to_datetime(merged_df['ExecutionTimeUTC'].dt.strftime('%Y-%m-%d %H:%M:%S'))
    merged_df['ExecutionTimeUTC'] = merged_df['ExecutionTimeUTC'].dt.tz_localize('UTC')

    cols = ['DeliveryStartUTC', 'TradeStartUTC', 'TradeEndUTC', 'ExecutionTimeUTC', 'VolumeMW', 'Price']
    merged_df_clean = merged_df[cols]

    # Apply the condition to filter the rows
    filtered_df = merged_df_clean[(merged_df_clean['ExecutionTimeUTC'] >= merged_df['TradeStartUTC']) & (merged_df_clean['ExecutionTimeUTC'] <= merged_df_clean['TradeEndUTC'])]
    # filter by execution time + delta

    return filtered_df

def calc_vwap_for_duration(intraday_transactions, requirements, duration, country):

    vwap_name = country + ' vwap '+ str(duration)
    vol_name = country + 'vol '+ str(duration)

    if intraday_transactions.empty:
        raise ValueError("filter_trades_df is empty, cannot calculate vwap filling nas.")
    filter_trades_df = filter_trades(intraday_transactions, requirements, duration)

    if filter_trades_df.empty:
        raise ValueError("filter_trades_df is empty, cannot calculate vwap filling nas.")

    groups = filter_trades_df.groupby(['DeliveryStartUTC', 'TradeStartUTC'])
    vwap_index = groups.apply(lambda x: np.average(x['Price'], weights=x['VolumeMW'])).reset_index(name=vwap_name)
    vwap_index[vol_name] = filter_trades_df.groupby(['DeliveryStartUTC', 'TradeStartUTC'])['VolumeMW'].sum().values

    return vwap_index

def calc_vwap_trades(requirements, country, product, durations):
    """
    Duplicates found for start and dateStarted only differenc is Lot number
    """
    database = client["EpexSpot"]
    collection = database["IntradayTransactions"]

    requirements['start'] = pd.to_datetime(requirements['start'])
    requirements['end'] = pd.to_datetime(requirements['end'])

    all_dates = requirements['start'].drop_duplicates()
    all_dates = all_dates.dt.to_pydatetime()
    query = {"Country": country, "DeliveryStartUTC": {"$in": all_dates.tolist()}, "Product": product}
    intraday_transactions = pd.DataFrame(list(collection.find(query)))

    vwap_indices = {}

    for duration in durations:
        try:

            vwap_indices[duration] = calc_vwap_for_duration(intraday_transactions, requirements, duration, country)
            vwap_indices[duration].set_index(['DeliveryStartUTC', 'TradeStartUTC'], inplace=True)
            store_vwapdb(vwap_indices[duration], country, product, duration)
        except ValueError as ve:
            print(ve)
            print('creating empty dataframe with nas')
            col_names = [country + ' vwap '+ str(duration), country + 'vol '+ str(duration)]
            vwap_index = pd.DataFrame(index=all_dates.tolist(), columns=col_names)
            vwap_indices[duration] = vwap_index

    return vwap_indices

def store_vwapdb(vwaps, country, product, duration):
        """
        generates the format ready for DAH Auction submission on test mongo db
        """
        db = client['testDB']

        vwaps.columns = ['VWAP', 'VolumeMWh']
        vwaps['Country'] = country
        vwaps['Product'] = product
        vwaps['Duration'] = duration
        vwaps['TradeEndUTC'] = vwaps.index.get_level_values(1) + pd.Timedelta(minutes=duration)
        # create end trade column
        vwaps = vwaps.round(2)
        vwaps.reset_index(inplace=True)
        vwaps = vwaps[["Country", "Product", "DeliveryStartUTC", "TradeStartUTC", "TradeEndUTC", "Duration", "VWAP", "VolumeMWh"]]

        data = vwaps.to_dict('records')

        collection = db['indexPricesFlows']
        collection.insert_many(data)

        return print('saved to db')


def get_vwapdb(requirements, country, product, duration):

    db = client['testDB']
    collection = db['indexPricesFlows']

    all_dates = requirements['start'].drop_duplicates()
    all_dates = pd.to_datetime(all_dates)

    query = {"Country": country, "Product": product, "Duration": {"$in": duration},
            "DeliveryStartUTC": {"$in": all_dates.tolist()}}
    project = {"_id": 0, "Country": 1, "Product": 1, "DeliveryStartUTC": 1, "TradeStartUTC": 1,
            "TradeEndUTC": 1, "Duration": 1, "VWAP": 1, "VolumeMWh": 1}
    vwap_indices = pd.DataFrame(list(collection.find(query, project)))

    vwap_indices['DeliveryStartUTC'] = pd.to_datetime(vwap_indices['DeliveryStartUTC'])
    vwap_indices['TradeStartUTC'] = pd.to_datetime(vwap_indices['TradeStartUTC'])
    vwap_indices['TradeEndUTC'] = pd.to_datetime(vwap_indices['TradeEndUTC'])

    vwap_indices.set_index(['DeliveryStartUTC', 'TradeStartUTC'], inplace=True)

    # reanme columns
    vwap_name = country + ' vwap'
    vol_name = country + 'vol'

    vwap_indices.rename({"VWAP": vwap_name, "VolumeMWh": vol_name}, axis = 1, inplace=True)
    # transform
    vwap_table = vwap_indices.pivot_table(index=['DeliveryStartUTC', 'TradeStartUTC'], columns='Duration', values=[vwap_name, vol_name])
    vwap_table.columns = vwap_table.columns.map(lambda x: ' '.join(map(str, x)))

    return vwap_table

def vwap_trayport(start, end, settings):
    """
    Referede API docs : https://www.trayport.com/en/support/daapi/index.html#/
    Reference Data API: https://www.trayport.com/en/support/refdataapi/index.html
    """

    start = pd.to_datetime(start).strftime('%Y-%m-%dT%H:%M:%SZ')
    end = pd.to_datetime(end).strftime('%Y-%m-%dT%H:%M:%SZ')


    url = settings['requirements_endpoint']
    headers = {'accept': 'application/json', 'X-API-KEY' : settings['api_key']}
    params = {'startedAfter': start, 'startedBefore': end, 'page': 1, 'perPage': 20}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

    vwap_trayport_df = pd.DataFrame(response.json())

    return vwap_trayport_df

def get_prices(start_datetime_utc, end_datetime_utc, country, auction):


    database = client['EpexSpot']
    collection = database['AuctionPrices']
    # build mongo db query for a date larger and lower than the start and end date

    query = {"Country": country,
            "Auction": auction,}
    query["StartTimeUTC"] = {"$lt": end_datetime_utc, "$gt": start_datetime_utc}
    projection = {"_id": 0, 'StartTimeUTC':1, 'Auction': 1, 'Country': 1, 'DeliveryDay': 1, 'Value': 1}
    forecast_document = list(collection.find(query, projection))

    spot_prices = pd.json_normalize(forecast_document)
    spot_prices.set_index('StartTimeUTC', inplace=True)
    spot_prices.index = spot_prices.index.tz_convert('Europe/London')
    spot_prices = spot_prices.rename(columns={'Value': 'spot_price'})
    spot_prices.index.rename('datetime_cet', inplace=True)

    return spot_prices

def get_bsads_diss(start, end):

    start_datetime_utc = start.tz_localize('utc')
    end_datetime_utc = end.tz_localize('utc')
    database = client['BMReports']
    collection = database['Disaggregated_BSADs']
    # build mongo db query for a date larger and lower than the start and end date

    query = {"StartTimeUTC": {"$lt": end_datetime_utc, "$gt": start_datetime_utc}}

    # projection = {"_id": 0, 'StartTimeUTC':1, "cost": 1, 'volume':1}
    forecast_document = list(collection.find(query))# , projection

    bsads = pd.json_normalize(forecast_document)
    bsads.set_index('StartTimeUTC', inplace=True)
    bsads.index = bsads.index.tz_convert('Europe/London')

    return bsads

def fx(start_utc, end_utc):

    index = pd.date_range(start_utc, end_utc, freq= 'h')
    index = index.tz_localize('UTC')
    fx_df = pd.DataFrame(index=index)
    end_utc = end_utc + pd.Timedelta(days=1)
    collection = client["Forex"]["ClosingFx"]
    query = {"date": {"$gte": start_utc, "$lt": end_utc}}
    record = list(
        collection.find(query)
    )
    data = pd.json_normalize(record)
    data.set_index('date', inplace=True)
    data = data['rate'].resample('60min').mean()
    fx_df = fx_df.join(data)
    fx_df = fx_df.fillna(method='ffill')
    fx_df.index = fx_df.index.tz_convert("Europe/London")

    return fx_df

def update_vwap_flows(requirements, intraday_settings):

    db = client['testDB']
    collection = db['indexPricesFlows']
    most_recent_doc = collection.find().sort("DeliveryStartUTC", DESCENDING).limit(1)
    most_recent_df  = pd.DataFrame(list(most_recent_doc))
    new_requirements = requirements[pd.to_datetime(requirements['start']) > most_recent_df['DeliveryStartUTC'].iloc[0]]

    if new_requirements.empty == False:

        calc_vwap_trades(new_requirements, 'FR', intraday_settings['product'], intraday_settings['duration'])
        calc_vwap_trades(new_requirements, 'NL', intraday_settings['product'], intraday_settings['duration'])
        calc_vwap_trades(new_requirements, 'BE', intraday_settings['product'], intraday_settings['duration'])

    return print('saved to db')

def get_vwap_flows(requirements, intraday_settings):

    update_vwap_flows(requirements, intraday_settings)
    vwap_fr_db = get_vwapdb(requirements, 'FR', intraday_settings['product'], intraday_settings['duration'])
    vwap_nl_db = get_vwapdb(requirements, 'NL', intraday_settings['product'], intraday_settings['duration'])
    vwap_be_db = get_vwapdb(requirements, 'BE', intraday_settings['product'], intraday_settings['duration'])

    requirements.rename(columns={'start': 'DeliveryStartUTC', 'dateStarted':'TradeStartUTC'}, inplace=True)
    requirements['TradeStartUTC'] = pd.to_datetime(requirements['TradeStartUTC'])
    requirements['DeliveryStartUTC'] = pd.to_datetime(requirements['DeliveryStartUTC'])
    requirements.set_index(['DeliveryStartUTC', 'TradeStartUTC'], inplace=True)

    requirements = requirements.join([vwap_fr_db, vwap_nl_db, vwap_be_db])

    return requirements

import time

def get_capacity_data_for_each_key(start_date, end_date, config, key):

    key_list = config[key]
    start_date = start_date.tz_localize('Europe/Paris').tz_convert('UTC').strftime("%Y-%m-%dT%H:%M")
    end_date = end_date.tz_localize('Europe/Paris').tz_convert('UTC').strftime("%Y-%m-%dT%H:%M")

    dataframes = []

    for key in key_list:

        url = config['capcity_auction']['endpoint']
        meta_id = key['id']
        headers = {'accept': 'application/json'}
        params = {'meta_data_id': meta_id, 'event_at_utc_from': start_date, 'event_at_utc_to': end_date}
        username = config['capcity_auction']['user']
        password = config['capcity_auction']['password']

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

# def get_capacity_data_for_each_key(start_date, end_date, config, key):

#     key_list = config[key]
#     start_date = start_date.tz_localize('Europe/Paris').tz_convert('UTC').strftime("%Y-%m-%dT%H:%M")
#     end_date = end_date.tz_localize('Europe/Paris').tz_convert('UTC').strftime("%Y-%m-%dT%H:%M")

#     dataframes = []

#     for key in key_list:

#         url = config['capcity_auction']['endpoint']
#         meta_id = key['id']
#         headers = {'accept': 'application/json'}
#         params = {'meta_data_id': meta_id, 'event_at_utc_from': start_date, 'event_at_utc_to': end_date}
#         username = config['capcity_auction']['user']
#         password = config['capcity_auction']['password']

#         try:
#             response = requests.get(url, headers=headers, params=params, auth=(username, password))
#             response.raise_for_status()
#         except requests.exceptions.RequestException as e:
#             print(f"Error: {e}")
#             continue

#         cap = pd.json_normalize(response.json())
#         cap['event_at_utc'] = pd.to_datetime(cap['event_at_utc'])
#         cap.set_index('event_at_utc', inplace=True)
#         cap.rename(columns={'value': key['data_key']}, inplace=True)
#         # Save each dataframe in the dictionary with the key's id as the key
#         # dataframes[key['data_key']] = cap

#         dataframes.append(cap)

#     combined_df = dataframes[0].join(dataframes[1:])

#     return combined_df

def get_enappsys_data(start, end, url):
    """
    start: Datetime BST
    end: Datetime BST
    apply exchange rate from GBP to EUR
    """
    end = end + pd.Timedelta(minutes=30)
    start_bst = start.tz_localize('Europe/Paris').tz_convert('Europe/London').strftime('%Y%m%d%H%M%S')
    end_bst = end.tz_localize('Europe/Paris').tz_convert('Europe/London').strftime('%Y%m%d%H%M%S')
    url = url.replace('initial', start_bst)
    url = url.replace('final', end_bst)
    df = pd.read_csv(url, index_col=0, parse_dates=True)
    df.index = pd.to_datetime(df.index, format='%d/%m/%Y %H:%M')
    # df.index = df.index.tz_localize('Europe/London')
    df = df[~df.index.duplicated(keep='first')]
    df.index = df.index.tz_localize('Europe/London', ambiguous = 'NaT')
    df = df.apply(pd.to_numeric, errors='coerce')
    col = ['Explicit Import Capacity Price (MW)', 'Explicit Export Capacity Price (MW)']
    df = df[col]
    col_names = ['BN_NL_GB', 'BN_GB_NL']
    df.columns = col_names

    fx_eur_gbp = fx(start, end)
    df = df.join(fx_eur_gbp)
    df['rate'] = df['rate'].fillna(method='ffill', limit=2)
    df['rate'] = df['rate'].fillna(method='bfill', limit=4)
    df['BN_NL_GB'] = df['BN_NL_GB']/df['rate']
    df['BN_GB_NL'] = df['BN_GB_NL']/df['rate']

    df.drop(columns=['rate'], inplace=True)

    return df

def get_intraday_cap_prices(start_date, end_date, config):
    """
    get intraday cap prices from jao and enappsys and join them into one dataframe
    convert it to long format
    """
    keys= config['intraday_cap_price']

    cap_prices = get_capacity_data_for_each_key(start_date, end_date, config, 'intraday_cap_price')
    cap_prices.index = cap_prices.tz_localize('UTC').index.tz_convert('Europe/London')

    mapping = {item['data_key']: item['display_name'] for item in keys}
    cap_prices.rename(columns=mapping, inplace=True)
    cap_30min = cap_prices.resample('30min').fillna(method='ffill', limit=1)

    try:
        britnet_cap = get_enappsys_data(start_date, end_date, config['intraday_cap_nl']['endpoint'])
        intraday_cap = cap_30min.join(britnet_cap)
    except Exception as e:
        print(f'Failed to get Britned Cap price: {e}')

    intraday_cap_long = intraday_cap.melt(ignore_index = False, var_name = 'interconnector', value_name = 'cap price')

    return intraday_cap_long

def agg_duplicated_requirements_volume(requirements):
    """agregate requirement volumes for the same delivery period
    """
    requirements_vwap = requirements.copy()
    volume_agg = requirements.groupby(requirements.index)['volume'].sum()
    requirements_vwap['volume'] = requirements_vwap.index.map(volume_agg)
    requirements_vwap = requirements_vwap[~requirements_vwap.index.duplicated(keep='first')]

    return requirements_vwap

# @st.cache(hash_funcs={MongoClient: id})
@st.cache_data
def get_data(start, end, config):

    bsads = get_bsads_diss(start, end)
    requirements = get_bsads_requirements(start, end, config)
    # rename to requirement publication time

    spot_prices_fr = get_prices(start, end, 'FR', 'FR-H')
    spot_prices_fr = spot_prices_fr.add_prefix('FR ')
    spot_prices_nl = get_prices(start, end, 'NL', 'NL-H')
    spot_prices_nl = spot_prices_nl.add_prefix('NL ')
    spot_prices_be = get_prices(start, end, 'BE', 'BE-H')
    spot_prices_be = spot_prices_be.add_prefix('BE ')
    spot_prices_gb = get_prices(start, end, 'GB', 'GB')
    spot_prices_gb = spot_prices_gb.add_prefix('GB ')
    fx_eur_gbp = fx(start, end)
    spot_prices_gb = spot_prices_gb.join(fx_eur_gbp)
    spot_prices_gb['GB spot price eur'] = spot_prices_gb['GB spot_price']/spot_prices_gb['rate']
    spot_prices_gb['GB spot price eur'] = spot_prices_gb['GB spot price eur'].round(1)

    intraday_cap = get_intraday_cap_prices(start, end, config)

    intraday_settings = config['intraday_market']

    requirements = get_vwap_flows(requirements, intraday_settings)

    requirements = requirements.round(1)
    requirements = requirements.reset_index().set_index('DeliveryStartUTC')
    requirements.index = requirements.index.tz_convert('Europe/London')
    requirements.index.rename('StartTimeUTC', inplace=True)

    requirements_vwap = agg_duplicated_requirements_volume(requirements)
    # requirements_vwap = requirements.copy()
    # drop duplicates by index
    # requirements_vwap = requirements_vwap[~requirements_vwap.index.duplicated(keep='first')]
    # filter columns that contains vol or vwap name
    col_vwap_vol = [column for column in requirements_vwap.columns if 'vol' in column or 'vwap' in column]
    col_vwap_vol.append('direction')
    requirements_vwap = requirements_vwap[col_vwap_vol]
    requirements_vwap.rename({"volume": "Volume Requirement"}, axis=1, inplace=True)

    # upsample in 30 min with repetition
    requirements_vwap = requirements_vwap.resample('30min').fillna(method='ffill', limit=1)


    list_spot = [spot_prices_fr['FR spot_price'], spot_prices_nl['NL spot_price'],
                spot_prices_be['BE spot_price'], spot_prices_gb[['GB spot_price', 'GB spot price eur']], fx_eur_gbp]
    list_spot = pd.concat(list_spot, axis=1)
    list_spot_30min = list_spot.resample('30min').fillna(method='ffill')
    data = bsads.join(list_spot_30min)
    data = data.join(requirements_vwap)


    ## add intraday cap price to interconnector
    data[['interconnector', 'company']] = data['assetId'].str.split('-', n=1, expand=True)
    data['interconnector'] = data['interconnector'].map(config['interconnectors_corridor'])
    ## do the merge join by index and interconnector column
    data['StartTimeUTC'] = data.index
    data = data.merge(intraday_cap, left_on=[data.index, 'interconnector'], right_on=[intraday_cap.index, 'interconnector'], how='left')
    data = data.drop(columns=['interconnector', 'company'])
    data.set_index('StartTimeUTC', inplace = True)

    return data, requirements

def vlook_up(df, col, mapping_dict, new_col):

    df['corresponding_col'] = df[col].map(mapping_dict)
    idx, cols = pd.factorize(df['corresponding_col'])
    df[new_col] = df.reindex(cols, axis=1).to_numpy()[np.arange(len(df)), idx]

    return df[new_col]

def lookup_vwap_interconnector(df_filtered, mapping_dict, durations):

    for duration in durations:
        mapping_dict_duration = mapping_dict.copy()
        for key in mapping_dict_duration:
            mapping_dict_duration[key] = '{}{}'.format(mapping_dict_duration[key], duration)

        df_filtered['vwap ' + str(duration)] = vlook_up(df_filtered, 'interconnetor', mapping_dict_duration, 'vwap ')

    return df_filtered


def process_data(bsads, config):
    """
    Process the data for the dashboard
    Create filter for companies and interconnectors
    Create filter for volume requirements
    Note: Interconnectors are first 3 letter of the assetId starting with I
    Calculate premiums and discounts
    Vlook UP : https://pandas.pydata.org/pandas-docs/version/1.3/user_guide/indexing.html#indexing-lookup
    refer to Looking up values by index/column labels

    """

    spec_companies = config['party']
    col = config['col']
    vwaps_column = [col for col in bsads.columns if 'vwap' in col]
    col.extend(vwaps_column)
    col.extend(['Volume Requirement', 'direction', 'cap price'])

    bsads['price'] = bsads['cost'] / bsads['volume']
    bsads = bsads[bsads['price'] <= 98999]
    bsads['spot'] = np.nan
    bsads['vwap'] = np.nan
    bsads = bsads[col]
    bsads.sort_index(ascending= False, inplace=True)

    selected_companies = st.multiselect('Select Companies', options=spec_companies, default=spec_companies)

    Interconnetor = list(set(config['interconnectors'].values()))
    selected_interconnector = st.multiselect('Select Interconnectors', options=Interconnetor, default=Interconnetor)

    selected_direction = st.multiselect('Select direction', options=['Offer', 'Bid'], default=['Offer', 'Bid'])

    bsads[['interconnetor', 'company']] = bsads['assetId'].str.split('-', n=1, expand=True)
    bsads['interconnetor'] = bsads['interconnetor'].replace(config['interconnectors'])

    df_filtered = bsads[(bsads['partyId'].isin(selected_companies)) & (bsads['interconnetor'].isin(selected_interconnector))]
    df_filtered = df_filtered[df_filtered['direction'].isin(selected_direction)]
    df_filtered['total_vol_absolute'] = df_filtered['Volume Requirement'].abs()
    #GRID REQUIREMENT
    volume_filter = st.number_input('Enter Minimum Volume requirement:', min_value=min(df_filtered['total_vol_absolute']), max_value=max(df_filtered['total_vol_absolute']), value=min(df_filtered['total_vol_absolute']))

    df_filtered = df_filtered[df_filtered['total_vol_absolute'] > volume_filter]
    dominant_competitor_vol = df_filtered.groupby("partyId")['total_vol_absolute'].sum()
    df_filtered['cum_vol_bsads'] = df_filtered[['assetId', 'total_vol_absolute']].groupby('assetId').cumsum()

    # Do the mapping of the variables here
    mapping_dict = config['intraday_market']['countries_mapping']
    duration_list = config['intraday_market']['duration']
    df_filtered = lookup_vwap_interconnector(df_filtered, mapping_dict, duration_list)
    mapping_spot = config['intraday_market']['countries_mapping_spot']
    df_filtered['spot'] = vlook_up(df_filtered, 'interconnetor', mapping_spot, 'spot')

    ## Calculate Premiums
    df_filtered['price_eur'] = df_filtered['price']/df_filtered['rate']
    df_filtered['intraday premium 15'] = df_filtered['price_eur'] - df_filtered['vwap 15']
    df_filtered['intraday premium 10'] = df_filtered['price_eur'] - df_filtered['vwap 10']
    df_filtered['intraday premium 5'] = df_filtered['price_eur'] - df_filtered['vwap 5']
    df_filtered['intraday premium 5'] = df_filtered['price_eur'] - df_filtered['vwap 5']
    df_filtered['spot premium'] = df_filtered['price_eur'] - df_filtered['spot']
    df_filtered['gb_conti spread'] = df_filtered['vwap 15'] - df_filtered['GB spot price eur']

    df_filtered.loc[df_filtered['direction'] == 'Bid', 'Volume Requirement'] = df_filtered.loc[df_filtered['direction'] == 'Bid', 'Volume Requirement']*-1

    return df_filtered

def calc_pnl(df_filtered, config):
    """
    Anlayze Profits and Premiums
    Analyze Volumes
    Limitations:
    * Pnl is calculated before cap cost we do not know the cap cost of every party
    * Spot and Intraday is a reference but actual cost of power procurement could be lower for asset Owner ie. EDF Nuclear generation cost"""

    RESOLUTION = 0.5 ## Half hour resolution
    df_filtered = loss_load(df_filtered, config)

    df_filtered['spot_profits'] = df_filtered['spread_spot']*df_filtered['volume'].abs()
    df_filtered['vwap_profits'] = df_filtered['spread_spot'] ## LEAVE TEMPORARY CHANGE TOMORROW
    df_filtered = calc_cap_cost(df_filtered)
    df_filtered['spot_profits_mwh'] = RESOLUTION*df_filtered['spot_profits']/df_filtered['volume'].abs()
    df_filtered['spot_profits'] = df_filtered['spot_profits']*RESOLUTION
    df_filtered['vwap_profits'] = df_filtered['vwap_profits']*RESOLUTION

    df_filtered.sort_index(inplace=True)
    df_filtered[['spot_cumpnl', 'vwap_cum_pnl']] = df_filtered.groupby('partyId')[['spot_profits', 'vwap_profits']].cumsum()
    df_filtered['spot_cumpnl_mwh'] = df_filtered.groupby('partyId')[['spot_profits_mwh']].cumsum()


    return df_filtered

def loss_load(df_filtered, config):

    # Calculate real time spread for Imports and Exports:
    df_filtered['losses'] = df_filtered['interconnetor'].map(config['flow_loss'])
    df_filtered['losses'] = df_filtered['losses']/100

    df_filtered['spread_spot'] = np.where(df_filtered['direction'] == 'Bid',
                                    df_filtered['spot'] - df_filtered['price_eur']* (1 + df_filtered['losses']),
                                    df_filtered['price_eur'] * (1 - df_filtered['losses']) - df_filtered['spot'])

    return df_filtered

def calc_cap_cost(df_filtered):

    ## company volume

    df_filtered['cap_cost'] = df_filtered['cap price']*df_filtered['volume'].abs()
    df_filtered['spot_profits'] = df_filtered['spot_profits'] - df_filtered['cap_cost']

    return df_filtered

def creat_confidence_interval(df_filtered, fig):

    data = df_filtered[['Volume Requirement', "spot premium", "volume"]].dropna(how='any')
    data = data.sort_values(by=['Volume Requirement'])
    x = data['Volume Requirement']
    y = data["spot premium"]
    w = data["volume"]

    # Line of best fit
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    line = slope*x + intercept
    # lr_w = sm.WLS(y, x, weights=w)
    # results = lr_w.fit()

    y_std = np.std(y)
    ci_upper = line + 1.96 * y_std
    ci_lower = line - 1.96 * y_std

    fig.add_trace(go.Scatter(x=x, y=line, mode='lines', name='Line of Best Fit'))
    fig.add_traces([go.Scatter(x =x, y = ci_lower,
                        mode = 'lines', line_color = 'rgba(0,0,0,0)',
                        showlegend = False),
                go.Scatter(x = x, y = ci_upper,
                        mode = 'lines', line_color = 'rgba(0,0,0,0)',
                        name = '95% confidence interval',
                        fill='tonexty', fillcolor = 'rgba(255, 0, 0, 0.2)')])

    return fig

def create_plots(df_filtered, requirements, config):

    df_filtered.index = df_filtered.index.tz_convert('Europe/Paris')
    col = config['col']
    col.insert(2, 'interconnetor')
    vwap_names =['price_eur', 'spot','cap_cost', 'spread_spot', 'spot_profits', 'vwap 15', 'vwap 10', 'vwap 5']
    premiums = ['intraday premium 15', 'intraday premium 10', 'intraday premium 5', 'spot premium', 'gb_conti spread'] 
    col.extend(vwap_names)
    col.extend(premiums)

    drop_cols = ['FR spot_price', 'NL spot_price', 'BE spot_price', 'FR vwap 15', 'FR vwap 10', 'FR vwap 5',
        'NL vwap 15', 'NL vwap 10', 'NL vwap 5', 'BE vwap 15', 'BE vwap 10', 'BE vwap 5']

    total_profits = df_filtered.groupby('partyId')['spot_profits'].sum()
    total_profits = total_profits/1000
    total_profits = total_profits.round(0)


    fig_x = px.scatter(df_filtered, x=df_filtered.index, y="price", color="partyId", hover_data=['assetId', 'settlementPeriod'])
    fig_premium = px.scatter(df_filtered, x=df_filtered.index, y="spot premium", color="partyId", hover_data=['partyId', 'settlementPeriod'])
    fig_vol = px.bar(df_filtered, x=df_filtered.index, y="volume", color="partyId", hover_data=['assetId', 'settlementPeriod'], barmode='stack')
    fig_profit = px.line(df_filtered, x=df_filtered.index, y="spot_cumpnl", color='partyId', hover_data=['assetId', 'settlementPeriod'])
    fig_profit_mwh = px.line(df_filtered, x=df_filtered.index, y="spot_cumpnl_mwh", color='partyId', hover_data=['assetId', 'settlementPeriod'])

    subplot_fig = sp.make_subplots(rows=5, cols=1, shared_xaxes=True)
    subplot_fig.update_layout(
    title_text="Grid Trades Prices & Premiums",
    height=2000,
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=-0.5,
        xanchor="right",
        x=1
    ))
    # Add y-axis titles
    subplot_fig.update_yaxes(title_text="Price", row=1, col=1)
    subplot_fig.update_yaxes(title_text="Spot Premium", row=2, col=1)
    subplot_fig.update_yaxes(title_text="Volume", row=3, col=1)
    subplot_fig.update_yaxes(title_text="Spot Cum Pnl", row=4, col=1)
    subplot_fig.update_yaxes(title_text="Spot Cum Pnl MWh", row=5, col=1)


    for trace in fig_x.data:
        subplot_fig.add_trace(trace, row=1, col=1)

    for trace in fig_premium.data:
        subplot_fig.add_trace(trace, row=2, col=1)

    for trace in fig_vol.data:
        subplot_fig.add_trace(trace, row=3, col=1)
    for trace in fig_profit.data:
        subplot_fig.add_trace(trace, row=4, col=1)
    for trace in fig_profit_mwh.data:
        subplot_fig.add_trace(trace, row=5, col=1)

    df_filtered = df_filtered[col].round(2)
    df_filtered.rename(columns={'settlementPeriod' : 'sp'}, inplace=True)
    df_filtered.index = df_filtered.index.tz_convert('Europe/Paris').tz_localize(None)
    df_filtered.index.name = 'Date Time CET'
    df_filtered.drop(columns=drop_cols, inplace=True)
    df_filtered.reset_index(inplace=True)
    st.dataframe(df_filtered)
    csv = df_filtered.to_csv(index=False)
    b_csv = csv.encode()
    st.download_button(label="Download data as CSV", data=b_csv, file_name="mydata.csv", mime="text/csv")
    st.plotly_chart(subplot_fig, use_container_width=True)

    pie_chart_fig = px.pie(values=total_profits.values, names=total_profits.index)
    pie_chart_fig.update_traces(hoverinfo='label+percent', textinfo='value')
    st.plotly_chart(pie_chart_fig)

    pie_per = px.pie(values=total_profits.values, names=total_profits.index)
    st.plotly_chart(pie_per)


    df_filtered['hour'] = df_filtered['Date Time CET'].dt.hour
    hours_list = list(range(0,24))
    selected_direction = st.multiselect('Select hour', options=hours_list, default=hours_list)
    df_filtered = df_filtered.loc[df_filtered['hour'].isin(selected_direction)]
    fig_vol_premium= px.scatter(df_filtered, x='Volume Requirement', y="spot premium", color='partyId')#, trendline='ols', hover_data=['assetId', 'settlementPeriod']

    fig_vol_premium = creat_confidence_interval(df_filtered, fig_vol_premium)
    st.plotly_chart(fig_vol_premium, use_container_width=True)



def BSAS_anaylsis_main():
    """
    Limit the query TO 3 months max as trayport only allows 3 months of data and faradyn grid trades only have 3 months of data
    max value limit for yesterday due to public trades published end of day solution is using trayport
    """
    file = os.path.join(BASE_DIR, "config_bsads.yaml")

    with open(file, 'r') as file:
        config = yaml.safe_load(file)


    min_date = pd.to_datetime(config['min_date'])

    today = pd.to_datetime('today')
    default_start_date = today - pd.Timedelta(days= 8)
    # default_start_date = pd.to_datetime('2024-01-01')


    st.title('BSADS Market Analysis')

    start_date, end_date = st.date_input('Select a date range', value=[default_start_date, today])#, min_value=min_date, max_value=today

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date) + pd.Timedelta(days=1)

    bsads, requirements = get_data(start_date, end_date, config)
    df_filtered = process_data(bsads, config)
    df_filtered = calc_pnl(df_filtered, config)
    create_plots(df_filtered, requirements, config)

# BSAS_anaylsis_main()

