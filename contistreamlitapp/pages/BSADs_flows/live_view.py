import os
import logging
import certifi
import yaml
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.subplots as sp
import streamlit as st
from pymongo import MongoClient
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Setting up logging
logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

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

def fx(start_utc, end_utc):

    start_utc = start_utc - pd.Timedelta(days=1)
    end_utc = end_utc + pd.Timedelta(days=1)
    collection = client["Forex"]["ClosingFx"]
    query = {"date": {"$gte": start_utc, "$lt": end_utc}}
    record = list(
        collection.find(query)
    )
    try:

        data = pd.json_normalize(record)
        data['date_cet'] = pd.to_datetime(data['date'].dt.date)
    except:
        ## BUGs fix is fx is missing take previous day fx
        start_utc = start_utc - pd.Timedelta(days=1)
        collection = client["Forex"]["ClosingFx"]
        query = {"date": {"$gte": start_utc, "$lt": end_utc}}
        record = list(
            collection.find(query)
        )
        data = pd.json_normalize(record)
        data['date_cet'] = pd.to_datetime(data['date'].dt.date)


    data.set_index('date_cet', inplace=True)
    data = data['rate'].resample('60min').mean()
    data = data.fillna(method='ffill')
    data.index = data.index.tz_localize("Europe/Paris", ambiguous = 'NaT')

    return data

def get_prices(start_datetime_utc, end_datetime_utc, country, auction):
    
    end_datetime_utc = end_datetime_utc + pd.Timedelta(days=1)
    database = client['EpexSpot']    
    collection = database['AuctionPrices']   

    query = {"Country": country,
            "Auction": auction,}
    query["StartTimeUTC"] = {"$lt": end_datetime_utc, "$gte": start_datetime_utc}
    projection = {"_id": 0, 'StartTimeUTC':1, 'Auction': 1, 'Country': 1, 'DeliveryDay': 1, 'Value': 1}     
    forecast_document = list(collection.find(query, projection))

    spot_prices = pd.json_normalize(forecast_document)
    spot_prices.set_index('StartTimeUTC', inplace=True)
    spot_prices.index = spot_prices.index.tz_convert('Europe/London')
    spot_prices = spot_prices.rename(columns={'Value': 'spot_price'})
    spot_prices.index.rename('datetime_cet', inplace=True)
        
    return spot_prices

def get_capacity_data_for_each_key(start_date, end_date, config, key):

    key_list = config[key]
    end_date = end_date + pd.Timedelta(days=1)
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

        try:
            response = requests.get(url, headers=headers, params=params, auth=(username, password))
            response.raise_for_status()  
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            continue

        cap = pd.json_normalize(response.json())
        cap['event_at_utc'] = pd.to_datetime(cap['event_at_utc'])
        cap.set_index('event_at_utc', inplace=True)
        cap.rename(columns={'value': key['data_key']}, inplace=True)
        # Save each dataframe in the dictionary with the key's id as the key
        # dataframes[key['data_key']] = cap
        
        dataframes.append(cap)
    
    combined_df = dataframes[0].join(dataframes[1:])

    return combined_df

def get_enappsys_data(url):
    df = pd.read_csv(url, index_col=0, parse_dates=True)
    df = df.iloc[1:]

    df.index = df.index.str.replace('[', '')
    df.index = df.index.str.replace(']', '')
    df.index = pd.to_datetime(df.index, format='%d/%m/%Y %H:%M')

    df = df.apply(pd.to_numeric, errors='coerce')


    return df

def get_fundamentals_data(start_date, end_date, settings):
    

    start_utc = start_date.tz_localize('utc').strftime('%Y%m%d%H%M')
    end_utc = end_date + pd.Timedelta(days=1)
    end_utc = end_utc.tz_localize('utc').strftime('%Y%m%d%H%M')

    urls = settings['fundamentals']

    for key in urls:
        url = urls[key]
        url = url.replace('startdate', start_utc)
        url = url.replace('enddate', end_utc)
        urls[key] = url



    demand = get_enappsys_data(urls['url_demand'])
    solar = get_enappsys_data(urls['url_solar'])
    solar = solar.add_suffix('_solar')
    solar_ng = get_enappsys_data(urls['url_solar_ng'])
    solar_ng = solar_ng.add_suffix('_solar')
    wind = get_enappsys_data(urls['url_wind'])
    wind = wind.add_suffix('_wind')

    list_fund = [solar, solar_ng['National Grid Forecast D1_solar'], wind]
    fundamentals = demand.join(list_fund)


    col = ['DEMAND FORECAST (NDF)', 'National Grid Forecast_wind', 'EnAppSys Forecast Trend-Adjusted_wind', 'D-1_solar', 'National Grid Forecast D1_solar']
    fundamentals = fundamentals[col]
    fundamentals = fundamentals[~fundamentals.index.duplicated(keep='first')]
    fundamentals.index = fundamentals.index.tz_localize('Europe/Paris', ambiguous = 'NaT')

    return fundamentals

def get_interconnector_req(start_datetime_utc, end_datetime_utc):
    """
    args:
        start_datetime_utc: datetime in UTC
        end_datetime_utc: datetime in UTC
    returns:
        bsads_interconnectors: pd.DataFrame with BSADS interconnector results"""
    
    database = client['BMReports']    
    collection = database['IC_Requirements']
    projection = {'_id': 0}

    query = {"StartTimeUTC":{"$lt": end_datetime_utc, "$gte": start_datetime_utc}} 
    forecast_document = list(collection.find(query))

    bsads_interconnectors = pd.json_normalize(forecast_document)
    bsads_interconnectors.set_index('StartTimeUTC', inplace=True)
    bsads_interconnectors.index = bsads_interconnectors.index.tz_convert('Europe/Paris')
    bsads_interconnectors.index.rename('datetime_cet', inplace=True)

    return bsads_interconnectors



def calcula_bsad_prob(volume, df_filtered):

    ## Calculate the probability of each BSAD occuring give the residual load
    ## Use first day ahead actuals to test the idea before using the proxy
    ## Add Day Ahead flow  to residual load use cap prices as proxy for flow direction and volume as proxy for flow size
    ## Use day Ahead commercial flow 
    ## include flows to the residual load
    volume['bsads_binary'] = np.sign(volume['Volume Required'])
    volume['bsads_binary'] = volume['bsads_binary'].fillna(0)
    # create buckets of residual load and groupby bucketed residual load and count the number of occurences of each prob_actual value
    volume['bucket'] = pd.cut(volume['residual load'], bins=range(-7000, 43000, 2000))
    df_filtered['bucket'] = pd.cut(df_filtered['residual load'], bins=range(-7000, 43000, 2000))
    probability = volume.groupby(['bucket', 'bsads_binary']).size().reset_index(name='counts')
    probability = probability.pivot_table(index = 'bucket', values = 'counts', columns = 'bsads_binary')
    probability['prob'] = 100*probability[1.0]/(probability[1.0] + probability[0.0])
    probability['prob'] = probability['prob'].round(1)

    df_filtered = df_filtered.merge(probability['prob'], how='left', left_on='bucket', right_index=True)
    df_filtered.drop(columns=['bucket'], inplace=True)
    col = df_filtered.pop(df_filtered.columns[-1])
    df_filtered.insert(0, col.name, col)

    return df_filtered

def get_data(start, end, config):

    start_utc = start.tz_localize('Europe/Paris').tz_convert('UTC')
    end_utc = end.tz_localize('Europe/Paris').tz_convert('UTC')
    start_bsasds = start_utc - pd.Timedelta(days=90)
    data = pd.DataFrame(index=pd.date_range(start_utc, end_utc, freq='60min'))
    data.index = data.index.tz_convert('Europe/Paris')
    ## Add an exeception here ! 

    ## Add an exeception here !
    try:

        cap_prices = get_capacity_data_for_each_key(start, end, config, 'cap_prices')
        cap_volume = get_capacity_data_for_each_key(start, end, config, 'cap_volume')
        cap_prices = cap_prices.join(cap_volume)
        cap_prices.index = cap_prices.index.tz_localize('UTC').tz_convert('Europe/Paris')
        cap_prices = cap_prices.drop(cap_prices.index[0])
        
    except:

        cap_prices = pd.DataFrame(index=pd.date_range(start_utc, end_utc, freq='60min'))
        cap_prices.index = cap_prices.index.tz_convert('Europe/Paris')
    
    try:
        
        bsads_interconnectors = get_interconnector_req(start_bsasds, end_utc)
        mask = (bsads_interconnectors.index > start_utc.tz_convert('Europe/Paris')) & (bsads_interconnectors.index < end_utc.tz_convert('Europe/Paris'))
        bsads_int_results = bsads_interconnectors.loc[mask]
        volume = bsads_interconnectors['Volume Required']
        
    except Exception as e:
        print(f"No data for this period. Error: {str(e)}")
        bsads_interconnectors = pd.DataFrame()

    spot_prices_gb = get_prices(start_utc, end_utc, 'GB', 'GB')
    spot_prices_gb = spot_prices_gb.add_prefix('GB ')
    spot_prices_gb.index = spot_prices_gb.index.tz_convert('Europe/Paris')
    fx_eur_gbp = fx(start_utc, end_utc)
    spot_prices_gb = spot_prices_gb.join(fx_eur_gbp)
    spot_prices_gb['rate'] = spot_prices_gb['rate'].fillna(method='bfill')
    spot_prices_gb['GB spot price eur'] = spot_prices_gb['GB spot_price']/spot_prices_gb['rate']
    spot_prices_gb['GB spot price eur'] = spot_prices_gb['GB spot price eur'].round(1)
    
    fundamentals = get_fundamentals_data(start_bsasds.tz_localize(None), end, config)


    data = data.join([cap_prices, spot_prices_gb[['GB spot_price', 'GB spot price eur']], fundamentals])
    volume = fundamentals.join(volume)

    return data, volume, bsads_int_results

def process_data(data, volume, bsads_int_results):
    
    df_filtered = data

    df_filtered = missing_data_check(df_filtered)
    df_filtered['D-1_solar'] = df_filtered['D-1_solar'].fillna(df_filtered['National Grid Forecast D1_solar'])


    df_filtered['residual load'] = df_filtered['DEMAND FORECAST (NDF)']  - df_filtered['National Grid Forecast_wind'] - df_filtered['D-1_solar']
    volume['residual load'] = volume['DEMAND FORECAST (NDF)']  - volume['National Grid Forecast_wind'] - volume['D-1_solar']

    df_filtered = calcula_bsad_prob(volume, df_filtered)

    bsads_int_results = bsads_int_results.reset_index()
    bsads_int_results = bsads_int_results.drop_duplicates(subset = ['datetime_cet', 'Auction ID'], keep = 'last')
    bsads_int_results.set_index('datetime_cet', inplace = True)
    
    return df_filtered, bsads_int_results

def missing_data_check(df_filtered):

    percentage_nas = 100*df_filtered.isna().sum()/len(df_filtered)
    percentage_nas = percentage_nas.loc[percentage_nas > 5]
    percentage_nas = percentage_nas.round(2)


    if (percentage_nas).any() > 5 :
            logging.info('variables missing data: ' + percentage_nas.index.astype(str) + "percentage nas: " + percentage_nas[0].astype(str))
            logging.info('filling Enappsys Day Ahead Solar when missing data with National Grid solar: ')


    return df_filtered

def show_logs():
    with open('app.log', 'r') as file:
        logs = file.read()
        
    return logs

def create_plots(df_filtered, bsads_int_results, config):

    keys= config['cap_prices']
    mapping = {item['data_key']: item['display_name'] for item in keys}

    keys= config['cap_volume']
    mapping_cap = {item['data_key']: item['display_name'] for item in keys}
    mapping.update(mapping_cap)

    df_cap = df_filtered.filter(regex ="jao_capacity_auction_market_data_prices")
    df_cap.rename(columns=mapping, inplace=True)
    df_cap_vol = df_filtered.filter(regex ="jao_capacity_auction_market_data_allocated_capacities")
    df_cap_vol.rename(columns=mapping_cap, inplace=True)
    df_filtered.rename(columns=mapping, inplace=True)

    df_filtered.index.names = ['Datetime CET']
    df_filtered.index = df_filtered.index.tz_localize(None)
    
    bsads_int_results.index = bsads_int_results.index.tz_localize(None)
    bsads_int_results['Published Time CET'] = bsads_int_results['PublishedTimeUTC'].dt.tz_convert('Europe/Paris').dt.tz_localize(None)
    bsads_int_results['End Time CET'] = bsads_int_results['EndTimeUTC'].dt.tz_convert('Europe/Paris').dt.tz_localize(None)
    bsads_int_results['BidDeadlineUTC'] = bsads_int_results['BidDeadlineUTC'].dt.tz_convert('Europe/Paris').dt.tz_localize(None)
    cols_int = ['Published Time CET', 'Notes', 'IFA1 Volume', 'IFA2 Volume', 'BN Volume',
                'NEMO Volume', 'EL Volume', 'Auction ID', 'Buy Sell', 'Volume Required',
                'Cleared Volume', 'Total Bid Volume', 'Auction Lot ID', 'Qualified IC', 'End Time CET',
                 'BidDeadlineUTC', 'Default Price', 'Clearing Price', 
                'Best Price','VWA Price']

    bsads_int_results = bsads_int_results[cols_int]

    fig = px.line(df_filtered, x=df_filtered.index , y = 'residual load', title='Residual load')
    fig_cap = px.line(df_cap)
    fig_cap_vol = px.line(df_cap_vol)

    subplot_fig = sp.make_subplots(rows=3, cols=1, shared_xaxes=True)
    subplot_fig.update_layout(
    title_text="Residual load and capacity Price",
    height=1000)
    subplot_fig.update_yaxes(title_text="Cap Price  Eur", row=1, col=1)
    subplot_fig.update_yaxes(title_text="Cap Allocated Vol MW ", row=2, col=1)
    subplot_fig.update_yaxes(title_text="Residual Load MW", row=3, col=1)


    for trace in fig_cap.data:
        subplot_fig.add_trace(trace, row=1, col=1)
    
    for trace in fig_cap_vol.data:
        subplot_fig.add_trace(trace, row=2, col=1)

    subplot_fig.add_trace(fig.data[0], row=3, col=1)

    logs = show_logs()

    st.dataframe(df_filtered)
    st.text_area("Logs", logs)
    st.plotly_chart(subplot_fig, use_container_width=True)
    st.dataframe(bsads_int_results)


def bsads_flow_dash():
    """
    Limit data to 1 month on Hotstorage
    """

    file = os.path.join(BASE_DIR, "config_live.yaml")

    with open(file, 'r') as file:
        config = yaml.safe_load(file)

    start = pd.to_datetime('today') 
    end = start + pd.Timedelta(days=2)


    # st.set_page_config(layout='wide')
    st.title('BSADS Market Live View')
    
    start_date, end_date = st.date_input('Select a date range', value=[start, end], max_value= end)

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    data, volume, bsads_int_results =  get_data(start_date, end_date, config)
    df_filtered, bsads_int_results = process_data(data, volume, bsads_int_results)
    create_plots(df_filtered, bsads_int_results, config)


bsads_flow_dash()
