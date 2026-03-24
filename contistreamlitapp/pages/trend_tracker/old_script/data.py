import os
import certifi
import pandas as pd
import pymongo
import numpy as np
from pymongo import MongoClient
import requests
import bson
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

def get_dah_positions(portfolio, product, country, start_utc, end_utc):

    collection = client["TradeData"]["TradeDeals"]
    record = list(
        collection.find(
            {"TradingPortfolio": portfolio,
            "Product": product},
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
    data = data.resample('30min').ffill(1) ##??????
    data = data.dropna()
    data = data._append(data.iloc[-1].rename(data.index[-1] + pd.Timedelta(minutes=30)))
    data.rename(columns={"Price": "DAH_Opening_Price", "VolumeMW": "DAH_Opening_Volume"}, inplace=True)
    data["DAH_Opening_Volume"] = data["DAH_Opening_Volume"].fillna(0)


    return data

def fx(start_utc, end_utc):

    end_utc = end_utc + pd.Timedelta(days=1)
    collection = client["Forex"]["ClosingFx"]
    query = {"date": {"$gte": start_utc, "$lt": end_utc}}
    record = list(
        collection.find(query)
    )
    data = pd.json_normalize(record)
    data.set_index('date', inplace=True)
    data = data['rate'].resample('30min').mean()
    data = data.fillna(method='ffill')
    data.index = data.index.tz_convert("Europe/Paris")

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

def get_vwap_old(country, duration, start_utc, end_utc, lead_time):
    """Get vwap data from MongoDB for a specific country and duration

    Args:
        country (str): country name
        duration (int): trading window duration in minutes
        start_utc (datetime): start datetime in UTC
        end_utc (datetime): end datetime in UTC
        lead_time (int): time to delivery in minutes

    """

    collection = client["EpexSpot"]['IndexPrices']
    query = {"Duration": duration, "Country": country}
    query["DeliveryStartUTC"] = {"$lt": end_utc, "$gte": start_utc}

    ## get data when DeliveryStartUTC is equal to TradeEndUTC
    # query["DeliveryStartUTC"] = {"$eq": "$TradeEndUTC"}
    #     
    # return specific fields
    projection = {"_id": 0, 'DeliveryEndUTC':1, 'DeliveryStartUTC':1,'TradeEndUTC':1,
    'TradeStartUTC':1,'Duration':1,'VWAP':1,'VolumeMWh':1}


    records = list(collection.find(query, projection)) 
    # convert to dataframe
    vwap = pd.json_normalize(records)
    ## drop duplicates DeliveryStartUTC keep last
    # drop rows with different dates on DeliveryStartUTC TradeEndUTC
    vwap['lead_time'] = vwap['DeliveryStartUTC'] - vwap['TradeEndUTC']
    # Get vwap with lead_time 3 hours
    vwap = vwap[vwap['lead_time'] == pd.Timedelta(minutes=lead_time)]


    vwap["DeliveryStartUTC"] = pd.to_datetime(vwap["DeliveryStartUTC"])
    vwap["DeliveryStartUTC"] = vwap["DeliveryStartUTC"].dt.tz_convert("Europe/Paris")
    vwap.set_index("DeliveryStartUTC", inplace=True)
    # rename columns
    vwap.rename(columns={"VolumeMWh": "vwap volume"}, inplace=True)
    vwap.index.rename('datetime_cet', inplace=True)

    return vwap

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
    freq_ = str(product_duration) + 'min' # BUGFIX identify freq
    vwap_index = vwap_index.asfreq(freq_)
    # rename columns
    vwap_index.rename(columns={"VolumeMWh": "vwap volume"}, inplace=True)
    vwap_index.index.rename('datetime_cet', inplace=True)
    vwap_index.drop('_id', axis= 1, inplace= True)

    return vwap_index
    

def get_enappsys_data(url):
    df = pd.read_csv(url, index_col=0, parse_dates=True)
    df = df.iloc[1:]
    # remove square brackets from Date column and convert to datetime
    df.index = df.index.str.replace('[', '')
    df.index = df.index.str.replace(']', '')
    df.index = pd.to_datetime(df.index, format='%d/%m/%Y %H:%M')#, format='%Y-%m-%d %H:%M:%S'

    #convert all columns to numeric
    df = df.apply(pd.to_numeric, errors='coerce')
    # rename value column to spot_price

    return df

def get_forecasts(country, auction, start_utc, end_utc, metadata_id):


    ids = metadata_id['fr']['dah']
    ids_list = list(ids.values())


    collection = client["TimeSeriesData"]["FeatureStore"]
    query = {"MetaDataId": {"$in": ids_list}} 
    query["CalculationTimeUTC"] = {"$lt": end_utc, "$gt": start_utc}

    # return specific fields
    projection = {"_id": 0,"MetaDataId": 1, "Value": 1, "CalculationTimeUTC": 1, "StartTimeUTC": 1}

    records = list(collection.find(query, projection))
    # convert to dataframe
    df = pd.json_normalize(records)


    df["StartTimeUTC"] = pd.to_datetime(df["StartTimeUTC"])
    df["StartTimeUTC"] = df["StartTimeUTC"].dt.tz_localize("UTC")

    fr_fundamentals = df.pivot(index="StartTimeUTC", columns="MetaDataId", values="Value")
    fr_fundamentals.reset_index(inplace=True)
    fr_fundamentals.set_index('StartTimeUTC', inplace=True)
    fr_fundamentals.index.rename('datetime_cet', inplace=True)
    fr_fundamentals.index = fr_fundamentals.index.tz_convert('Europe/Paris').tz_localize(None)

    return fr_fundamentals

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

# get transactions data
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

def get_flow_cap(corridor, trade_type, start_utc, end_utc):

    if corridor == "None":

        cols = ['Cap Price', 'Cap Volume']
        index = pd.date_range(start_utc, end_utc, freq='h', tz='utc')
        index = index.tz_convert('Europe/Paris')
        print('we do not flow for IE at the moment')
        return pd.DataFrame(index=index, columns=cols)
    
    collection = client["TradeData"]["PowerCapacity"]
    query = {"$and": [{"Corridor": {"$in":corridor}},
                        {"TradeType": trade_type},
                        {"CommodityFamily": {"$in": ["POWER"]}},
                        {"StartTimeUTC": {"$gte": start_utc}},
                        {"EndTimeUTC": {"$lte": end_utc}}]}
    fields = {"_id": 0,
                "Border": 1,
                "Corridor": 1,
                "TradeType": 1,
                "StartTimeUTC": 1,
                "EndTimeUTC": 1,
                "ExecutionTimeUTC": 1,
                "Price":1,
                "VolumeMW":1,
                "Side":1
                }
    data = pd.DataFrame(list(collection.find(query, fields)))


    data["StartTimeUTC"] = pd.to_datetime(data["StartTimeUTC"])
    data.set_index('StartTimeUTC', inplace=True)
    data.index= data.index.tz_convert('Europe/Paris')
    data["EndTimeUTC"] = pd.to_datetime(data["EndTimeUTC"])
    data["EndTimeUTC"] = data["EndTimeUTC"]
    
    
    data.index.rename('datetime_cet', inplace=True)
    # add prefix CAP to specific columns Price Side VolumeMW
    col = ['Price', 'VolumeMW', 'Corridor']
    capacity =  data[col].add_prefix('CAP_')

    capacity = capacity.pivot_table(index = "datetime_cet", columns='CAP_Corridor', values=['CAP_Price', 'CAP_VolumeMW'])
    capacity.columns = [' '.join(col).strip() for col in capacity.columns.values]
    #test = capacity.pivot(columns = 'CAP_Corridor')

    return capacity

def get_flow_nominations(nomination_settings, start_utc, end_utc):
    '''
    Get flow nominations from database
    wthc out for currency
    check start and end time execution time and cable
    '''
    

    portfolio = nomination_settings["portfolio"]
    tradetype = nomination_settings["tradetype"]
    countryfrom = nomination_settings["CountryFrom"]
    countryto = nomination_settings["CountryTo"]

    if countryfrom == "IE":

        index = pd.date_range(start_utc, end_utc, freq='h', tz='utc')
        index = index.tz_convert('Europe/Paris')
        cols = ['Nomination_Price', 'Nomination_Side', 'Nomination_VolumeMW', 'Nomination_Cable',
                'Nomination_Country', 'Nomination_CountryFrom', 'Nomination_CountryTo']
        print("We do not have nominations for Ireland")

        return pd.DataFrame(columns=cols, index=index)

    collection_flow = client["TradeData"]["TradeDeals"]
    
    record = list(collection_flow.find(
            {"TradingPortfolio":  portfolio,
            "TradeType": tradetype,
            "CountryFrom": {"$in":countryfrom},
            "CountryTo": {"$in":countryto},
            "StartTimeUTC": {"$gte": start_utc},
            "EndTimeUTC": {"$lte": end_utc},
            "CommodityFamily": "POWER"},
            sort=[("StartTimeUTC", pymongo.ASCENDING)]
        )
    )
    data = pd.json_normalize(record)
    data["StartTimeUTC"] = pd.to_datetime(data["StartTimeUTC"])
    data.set_index('StartTimeUTC', inplace=True)
    data.index= data.index.tz_convert('Europe/Paris')
    data["EndTimeUTC"] = pd.to_datetime(data["EndTimeUTC"])
    data["EndTimeUTC"] = data["EndTimeUTC"]
    data.index.rename('datetime_cet', inplace=True)

    col = ['Price', 'Side', 'VolumeMW', 'Cable', "Country",	"CountryFrom", "CountryTo", 'ExecutionTimeUTC']
    # drop duplicates
    data = data.sort_values(['datetime_cet','ExecutionTimeUTC'])
    last_nomination = data.loc[~data.index.duplicated(keep='last')]

    last_nomination =  last_nomination[col].add_prefix('Nomination_')
    # nomination_flow = nomination_flow.pivot_table(index = "datetime_cet", columns='Nomination_Cable', values=['Nomination_Price', 'Nomination_Side', 'Nomination_VolumeMW'])
    # nomination_flow.columns = [' '.join(col).strip() for col in nomination_flow.columns.values]

    return last_nomination

def get_intraday_i(url_data):

    # url_date = url_data.replace("2023-06-07", start_utc.strftime("%Y-%m-%d"))
    headers = {'accept': 'application/json'}
    session = requests.Session()

    # Set up retry with backoff
    retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[ 500, 502, 503, 504 ])

    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    response = requests.get(url_data, headers=headers)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        intraday_position = pd.DataFrame(data)
    else:
        print(f"Request failed with status code {response.status_code}")
    
    intraday_position.set_index('StartTimeUTC', inplace=True)
    intraday_position.index = pd.to_datetime(intraday_position.index).tz_convert('Europe/Paris')

    return intraday_position

def get_intraday_positions(country, product, start_utc, end_utc, url):

    # if country == "IE":

    #     print("Ireland is not supported yet returning empty dataframe")
    #     index = pd.date_range(start_utc, end_utc, freq='h', tz='utc')
    #     index = index.tz_convert('Europe/Paris')
    #     col = ['OpenPosition', 'ClosedPosition', 'BuyVWAP', 'SellVWAP']

    #     return pd.DataFrame(index=index, columns=col)
    try:

        dates = pd.date_range(start_utc, end_utc, freq='D', tz='utc')    
        url_c = url.replace("FR", country)
        url_data = url_c.replace("Day-Ahead", product)

        # do a for loop to get the data for each day and append it to a list
        intraday_positions_l = pd.DataFrame()
        for i in range(len(dates)):
            url_date = url_data.replace("2023-06-07", dates[i].strftime("%Y-%m-%d"))
            intraday_positions_l =intraday_positions_l._append(get_intraday_i(url_date))
        
        # col = ['OpenPosition', 'ClosedPosition', 'BuyVWAP', 'SellVWAP']
        intraday_positions_l = intraday_positions_l#[col]
        intraday_positions_l = intraday_positions_l.dropna()
        intraday_positions_l = intraday_positions_l.resample('30min').ffill(1)
        # copy last row and add 30 min to index
        intraday_positions_l = intraday_positions_l._append(intraday_positions_l.iloc[-1].rename(intraday_positions_l.index[-1] + pd.Timedelta(minutes=30)))
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        dates_requested = pd.date_range(start_utc, end_utc, freq='h')
        col_names = ['BuyValue', 'SellValue', 'BuyQty', 'SellQty', 'OpenPosition', 'ClosedPosition', 'BuyVWAP', 'SellVWAP']
        intraday_positions_l = pd.DataFrame(index = dates_requested, columns= col_names)
        intraday_positions_l.index = intraday_positions_l.index.tz_convert('Europe/Paris')
        
    return intraday_positions_l

# calculate vwap
def calc_vwap(from_utc, to_utc, country, product, lead_time): 
    """
    Refactor with proper labels 
    """
    try:
        intraday_trades = get_transactions(from_utc, to_utc, country, product)
        intraday_trades['lead_time'] = intraday_trades['StartTimeUTC'] - intraday_trades['ExecutionTimeUTC']
        intraday_trades = intraday_trades[intraday_trades['lead_time'] >= pd.Timedelta(minutes =lead_time)]

        intraday_trades['trade_value'] = intraday_trades['Price']*intraday_trades['VolumeMW']

        groups_wap = intraday_trades.groupby(['DeliveryStartUTC'])
        trade_n_min = groups_wap.sum()[['trade_value','VolumeMW']]

        trade_n_min['vwap'] = trade_n_min['trade_value']/trade_n_min['VolumeMW']
        trade_n_min = trade_n_min.reset_index()

        # convert DeliverStartUTC to CET set as an index and add country suffx with Country
        trade_n_min = trade_n_min.set_index('DeliveryStartUTC')
        trade_n_min.index = trade_n_min.index.tz_localize('UTC').tz_convert('Europe/Paris')
        trade_n_min.index.rename('datetime_cet', inplace=True)
        trade_n_min = trade_n_min.add_suffix('_'+country)
        trade_n_min.drop(columns=['trade_value_'+country], inplace=True)

    except:
        date_range = pd.date_range(from_utc, to_utc, freq='60min', tz='utc')
        trade_n_min = pd.DataFrame(index=date_range, columns=['VolumeMW_'+country, 'vwap_'+country])


    return trade_n_min

def calc_vwap_gc(from_utc, to_utc, country, product, lead_time ,duration=34*60): 
    """
    Refactor with proper labels 
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
    trade_n_min.index = trade_n_min.index.tz_localize('UTC').tz_convert('Europe/Paris')
    trade_n_min.index.rename('datetime_cet', inplace=True)
    trade_n_min = trade_n_min.add_suffix('_'+country)
    trade_n_min.drop(columns=['trade_value_'+country], inplace=True)


    return trade_n_min

def get_own_vwap(start_utc, end_utc, country, product, portfolio):

    if country == "IE":

        print("Ireland is not supported yet returning empty dataframe")
        index = pd.date_range(start_utc, end_utc, freq='h', tz='utc')
        index = index.tz_convert('Europe/Paris')
        col = ['VolumeMW_'+ country +' Buy',  'vwap_' +country + ' Buy', 'VolumeMW_'+ country +' Sell',  'vwap_' +country + ' Sell']

        return pd.DataFrame(index=index, columns=col)
    
    collection = client["TradeData"]["TradeDeals"]


    record = list(collection.find(
            {"TradingPortfolio":  {"$in": portfolio},
            "Product": product,
            "StartTimeUTC": {"$gte": start_utc},
            "EndTimeUTC": {"$lte": end_utc},
            "CommodityFamily": "POWER"},
            sort=[("StartTimeUTC", pymongo.ASCENDING)]
        )
    )

    data = pd.json_normalize(record)


    data['StartTimeUTC'] = pd.to_datetime(data['StartTimeUTC'])
    # create a buy dataframe by filtering on the side sell
    buy = data[data['Side'] == 'B']
    sell = data[data['Side'] == 'S']
    vwap_buy = calc_vwap_internal(buy, country)
    vwap_buy = vwap_buy.add_suffix(' Buy')
    vwap_sell = calc_vwap_internal(sell, country)
    vwap_sell = vwap_sell.add_suffix(' Sell')

    vwap = pd.DataFrame(index=data['StartTimeUTC'].unique())

    vwap = vwap.join([vwap_buy, vwap_sell])
    vwap.index = vwap.index.tz_localize('UTC').tz_convert("Europe/Paris")
    vwap.index.rename('StartTimeCET', inplace=True)

    return vwap

def get_own_vwap_gb(start_utc, end_utc, country, product, portfolio):

    collection = client["TradeData"]["TradeDeals"]

    record = list(collection.find(
            {"TradingPortfolio":  {"$in": portfolio},
            "Product": {"$in": product},
            "StartTimeUTC": {"$gte": start_utc},
            "EndTimeUTC": {"$lte": end_utc}},
            sort=[("StartTimeUTC", pymongo.ASCENDING)]
        )
    )

    data = pd.json_normalize(record)

    data['StartTimeUTC'] = pd.to_datetime(data['StartTimeUTC'])
    data['EndTimeUTC'] = pd.to_datetime(data['EndTimeUTC'])
    data['ExecutionTimeUTC'] = pd.to_datetime(data['ExecutionTimeUTC'])

    data_h = split_trades_to_half_hours(data)
    data_h.set_index('StartTimeUTC', inplace=True)
    # replace Side column with B for Buy S for Sell
    data_h['Side'] = data_h['Side'].replace({'B': 'Buy', 'S': 'Sell'})

    vwap = data_h.groupby('Side').resample('H').apply(vwap_opt)
    vwap = vwap.unstack(level=0)
    vwap.columns = [' '.join(col).strip() for col in vwap.columns.values]
    vwap = vwap.add_suffix("_"+country)

    vwap.index = vwap.index.tz_localize('UTC').tz_convert("Europe/Paris")

    return vwap


def vwap_opt(df):
    q = df['VolumeMW']
    p = df['Price']
    vwap = (p * q).sum() / q.sum()
    volume = q.sum()
    return pd.Series([vwap, volume], index=['vwap', 'VolumeMW'])

# Apply the function to each hourly group in the DataFrame.


def split_trades_to_half_hours(trades):

    hour_flag = (trades["EndTimeUTC"] - trades["StartTimeUTC"]) > pd.Timedelta(minutes=30)
    hour_trades = trades[hour_flag]

    if not hour_trades.empty:
        dummy_trade_list = []
        for i, trade in hour_trades.iterrows():

            number_of_half_hours = int(
                (trade["EndTimeUTC"] - trade["StartTimeUTC"]).total_seconds() // 60 // 30
            )

            for j in range(number_of_half_hours):
                place_holder_trade = trade.copy()
                place_holder_trade["StartTimeUTC"] = place_holder_trade[
                                                        "StartTimeUTC"
                                                    ] + pd.Timedelta(minutes=30 * j)
                place_holder_trade["EndTimeUTC"] = place_holder_trade[
                                                    "StartTimeUTC"
                                                ] + pd.Timedelta(minutes=30)
                place_holder_trade["_id"] = bson.objectid.ObjectId()
                dummy_trade_list.append(place_holder_trade)

        dummy_trades = pd.DataFrame(dummy_trade_list)

        trades = trades[~hour_flag]

        trades = pd.concat([trades, dummy_trades])
        trades.sort_values("ExecutionTimeUTC", inplace=True)

    return trades.reset_index(drop=True)


def calc_vwap_internal(intraday_trades, country):

    
    intraday_trades['trade_value'] = intraday_trades['Price']*intraday_trades['VolumeMW']
    groups_wap = intraday_trades.groupby(['StartTimeUTC'])
    trade_n_min = groups_wap.sum()[['trade_value','VolumeMW']]
    trade_n_min['vwap'] = trade_n_min['trade_value']/trade_n_min['VolumeMW']
    trade_n_min = trade_n_min.reset_index()
    trade_n_min.set_index('StartTimeUTC', inplace=True)
    trade_n_min = trade_n_min.add_suffix('_'+country)
    trade_n_min.drop(columns=['trade_value_'+country], inplace=True)

    return trade_n_min

def cal_olhc(groups_wap):
    

    functions = [np.mean, np.std, np.median, 'quantile']
    trade_n_min_olhc = groups_wap.agg({'Price':functions, 'VolumeMW':['sum']})

    return trade_n_min_olhc