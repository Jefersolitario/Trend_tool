import certifi
import yaml
import numpy as np
import pandas as pd
from pymongo import MongoClient, DESCENDING
import requests


# def mongo_client():
#     return MongoClient(
#             f"mongodb://app_power_dashboard:jQl6TZMfYe61Brbs@dev1-shard-00-00.uvhb7.mongodb.net:27017,dev1-shard-00-01.uvhb7.mongodb.net:27017,dev1-shard-00-02.uvhb7.mongodb.net:27017/test?authSource=admin&replicaSet=atlas-k6fhv2-shard-0&ssl=true",
#             tz_aware=True,
#             w="majority",
#             readpreference="primary",
#             journal=True,
#             wTimeoutMS=60000,
#             connect=False,
#             tlsCAFile=certifi.where(),
#             maxPoolSize=200,
#         )


# client = mongo_client()


def auction_requirements(auction_id, settings):


    url = settings['auction_requirements']
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

    start = pd.to_datetime(start).strftime('%Y-%m-%dT%H:%M:%SZ')
    end = pd.to_datetime(end).strftime('%Y-%m-%dT%H:%M:%SZ')

    url = settings['requirements_endpoint']
    headers = {'accept': 'application/json'}
    params = {'startedAfter': start, 'startedBefore': end, 'page': 1, 'perPage': 200}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
    
    bsads_requirements = pd.DataFrame(response.json()['results'])

    bsads_requirements['dateStarted'] = pd.to_datetime(bsads_requirements['dateStarted'])
    bsads_requirements.rename(columns={'dateStarted': 'publicationTime'}, inplace=True)

    requirements_details = bsads_requirements['id'].apply(lambda x: auction_requirements(x, settings)).tolist()
    requirements_details = pd.concat(requirements_details, ignore_index=True)
    
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
    filter_trades_df = filter_trades(intraday_transactions, requirements, duration) 
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

        vwap_indices[duration] = calc_vwap_for_duration(intraday_transactions, requirements, duration, country)
        vwap_indices[duration].set_index(['DeliveryStartUTC', 'TradeStartUTC'], inplace=True)
        store_vwapdb(vwap_indices[duration], country, product, duration)

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

    end_utc = end_utc + pd.Timedelta(days=1)
    collection = client["Forex"]["ClosingFx"]
    query = {"date": {"$gte": start_utc, "$lt": end_utc}}
    record = list(
        collection.find(query)
    )
    data = pd.json_normalize(record)
    data.set_index('date', inplace=True)
    data = data['rate'].resample('60min').mean()
    data = data.fillna(method='ffill')
    data.index = data.index.tz_convert("Europe/London")

    return data

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
