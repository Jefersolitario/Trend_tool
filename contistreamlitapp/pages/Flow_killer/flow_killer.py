import certifi
import yaml
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.subplots as sp
import streamlit as st
from pymongo import MongoClient, DESCENDING
import time
import matplotlib.pyplot as plt
import requests
import cProfile
import pstats
import io
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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

def get_xbid_transaction(from_utc, to_utc, country, product):

    buy_delivery_areas = list(country.values())
    sell_delivery_areas =  list(country.values())

    collection = client["EpexSpot"]["AutotraderPublicTrades"]
    query = {"product_type": product, 
            "delivery_start": {"$gte": from_utc, "$lte": to_utc},
            "buy_delivery_area": {"$in": buy_delivery_areas}}
    
    query2 = {"product_type": product, 
            "delivery_start": {"$gte": from_utc, "$lte": to_utc},
            "sell_delivery_area": {"$in": buy_delivery_areas}}
    # query = {"product_type": product, 
    #     "delivery_start": {"$gte": from_utc, "$lte": to_utc},
    #     "$or": [
    #         {"buy_delivery_area": {"$in": buy_delivery_areas}},
    #         {"sell_delivery_area": {"$in": buy_delivery_areas}}
    #     ]}
            # "$or": [
            # {
            #     "sell_delivery_area": {"$in": sell_delivery_areas},
            #     # "buy_delivery_area": {"$ne": {"$regex": country}},
            # },
            # {
            #     "buy_delivery_area": {"$in": buy_delivery_areas},
            #     # "sell_delivery_area": {"$ne": {"$regex": country}},
            # },],
            # }

    fields = {
        "_id": 0,
        "trade_id": 1,
        "product_type": 1,
        "buy_delivery_area": 1,
        "sell_delivery_area": 1,
        "delivery_start": 1,
        "execution_time": 1,
        "price": 1,
        "quantity": 1,
    }

    trades = list(collection.find(query, fields))
    trade_data = pd.DataFrame(trades)
    trades_2 = list(collection.find(query2, fields))
    trade_data_2 = pd.DataFrame(trades_2)

    trade_data = pd.concat([trade_data, trade_data_2])
    trade_data = trade_data.drop_duplicates()

    return trade_data
def get_latest_transaction(from_utc, to_utc, country, product):

    collection = client["EpexSpot"]["AutotraderPublicTrades"]
    query = {"product_type":{"$in": product}, "delivery_start": {"$gte": from_utc, "$lte": to_utc}}

    fields = {
        "_id": 0,
        "product_type": 1,
        "delivery_start": 1,
        "execution_time": 1,
        "price": 1,
        "quantity": 1,
    }

    trades = list(collection.find(query, fields))
    trade_data = pd.DataFrame(trades)

    return trade_data

def get_fx(start_utc, end_utc):

    end_utc = end_utc + pd.Timedelta(days=1)
    start_utc = start_utc - pd.Timedelta(days=1)
    collection = client["Forex"]["ClosingFx"]
    query = {"date": {"$gte": start_utc, "$lt": end_utc}}
    record = list(
        collection.find(query)
    )
    data = pd.json_normalize(record)
    data.set_index('date', inplace=True)
    data = data['rate'].resample('60min').mean()
    data = data.fillna(method='ffill')

    extended_index = pd.date_range(start=data.index[0], end=data.index[-1] + pd.Timedelta(days=1), freq='60min')
    data = data.reindex(extended_index, method='ffill')

    return data

@st.cache
def get_eur_to_gbp():
   url = "https://open.er-api.com/v6/latest/EUR"
   response = requests.get(url)

   if response.status_code == 200:
       data = response.json()
       gbp_rate = data['rates']['GBP']
       return gbp_rate
   else:
       print(f"Error {response.status_code}: {response.text}")
       return None

def get_capacity_data_for_each_key(start_date, end_date, config, key):

    key_list = config[key]
    end_date = end_date + pd.Timedelta(days=1)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M")

    dataframes = []
    combined_df = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='60min'))

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
                time.sleep(0.2)  # wait for 0.2 second before next retry
        else:
            print("Failed to retrieve data after 5 attempts. Moving to the next key.")
            continue

        cap = pd.json_normalize(response.json())
        cap['event_at_utc'] = pd.to_datetime(cap['event_at_utc'])
        cap.set_index('event_at_utc', inplace=True)
        cap.rename(columns={'value': key['data_key']}, inplace=True)
        # Save each dataframe in the dictionary with the key's id as the key
        # dataframes[key['data_key']] = cap
        
        dataframes.append(cap)


    combined_df = combined_df.join(dataframes)
    combined_df = combined_df.dropna(how='all')

    return combined_df

def get_data(start_date, end_date, config):

    gb_product = config["intraday_trades"]["GB"]["intraday_products_gb"]
    product_xbid = config["intraday_trades"]["product"]
    country = config["intraday_trades"]["areas"]

    last_gb_trade = get_latest_transaction(start_date, end_date, "GB", gb_product)

    last_conti_trade = get_xbid_transaction(start_date, end_date, country, product_xbid)

    available_capacity = get_capacity_data_for_each_key(start_date, end_date, config, 'intraday_cap')

    return last_gb_trade, last_conti_trade, available_capacity

def get_gb_latest_trade(last_gb_trade, from_utc, to_utc):

    last_gb_trade.sort_values(by=["delivery_start", "execution_time"], ascending=False, inplace=True)
    latest_trade = last_gb_trade.drop_duplicates(subset = ["delivery_start", "product_type"])#, inplace=True
    latest_trade_pivot = latest_trade.pivot(index="delivery_start", columns="product_type", values=["quantity", "price"])
    latest_trade_pivot.columns = ["_".join(col) for col in latest_trade_pivot.columns]
    latest_trade_table = latest_trade_pivot.reindex(pd.date_range(from_utc, to_utc, freq="30min"))

    return latest_trade_table

def get_conti_latest_trade(conti_trade, config):
    """ Find the most recent trade
    1. Sort by the most recent trade and product
    2. map country to aread code
    3. drop duplicates get latest trade
    4. format table
    """

    conti_trade.sort_values(by=["delivery_start", "execution_time"], inplace=True)
    inverted_dict = {v: k for k, v in config["intraday_trades"]["areas"].items()}
    
    conti_trade['country_sell'] = conti_trade['sell_delivery_area'].map(inverted_dict)
    conti_trade['country_buy'] = conti_trade['buy_delivery_area'].map(inverted_dict)
    conti_trade['country'] = conti_trade['country_sell'].fillna(conti_trade['country_buy'])
    drop_dupl_col = ["delivery_start", "country"]
    latest_xbid_trade = conti_trade.drop_duplicates(subset = drop_dupl_col, keep = "last")
    latest_xbid_trade = latest_xbid_trade.pivot(index="delivery_start", columns="country", values=["price"])
    latest_xbid_trade.columns = latest_xbid_trade.columns.droplevel(0)

    return latest_xbid_trade

def get_uk_product_label(latest_trade_table):
    """ Get the product labels for the UK market"""

    latest_trade_table.index = latest_trade_table.index.tz_convert("Europe/Paris")
    latest_trade_table['London_time'] = latest_trade_table.index.tz_convert("Europe/London")
    latest_trade_table['sp'] = 1 + latest_trade_table['London_time'].dt.hour * 2 + latest_trade_table['London_time'].dt.minute // 30
    latest_trade_table['sp'] = latest_trade_table['sp'].astype(str) + '-' + (latest_trade_table['sp'] + 1).astype(str)
    latest_trade_table['4HR'] = 1 + latest_trade_table.index.hour // 4
    latest_trade_table['subperiod'] = latest_trade_table.index.hour % 4 // 2
    latest_trade_table['2HR'] = latest_trade_table['4HR'].astype(str) + latest_trade_table['subperiod'].map({0: 'A', 1: 'B'})
    # if a column does not exist, create it and fill it with 0 use regex to find the column
    latest_trade_table.drop(columns=["London_time", "subperiod"], inplace=True)
    latest_trade_table.set_index(['4HR', '2HR', 'sp'], append = True, inplace=True)

    return latest_trade_table

def create_xbid_average(latest_trade_table, col):

    group_4hr = [latest_trade_table.index.get_level_values("Date CET").date, latest_trade_table.index.get_level_values('4HR')]
    block_average_4h = latest_trade_table.groupby(group_4hr).mean()[col]
    block_average_4h.dropna(inplace=True)
    block_average_4h.index.set_names('date', level=0, inplace=True)

    group_2hr = [latest_trade_table.index.get_level_values("Date CET").date, latest_trade_table.index.get_level_values('2HR')]
    block_average_2h = latest_trade_table.groupby(group_2hr).mean()[col]
    block_average_2h.dropna(inplace=True)
    block_average_2h.index.set_names('date', level=0, inplace=True)


    # merge average
    latest_trade_table['date'] = latest_trade_table.index.get_level_values("Date CET").date
    latest_trade_table = latest_trade_table.reset_index()

    latest_trade_table = latest_trade_table.merge(block_average_4h.reset_index(), on = ['date', '4HR'], how='left', suffixes=('', '_4HR'))
    latest_trade_table = latest_trade_table.merge(block_average_2h.reset_index(), on = ['date', '2HR'], how='left', suffixes=('', '_2HR'))



    latest_trade_table = latest_trade_table.round(2)
    # set multiindex
    latest_trade_table.set_index(['Date CET', '4HR', '2HR', 'sp'], inplace=True)
    latest_trade_table.drop(columns=["date"], inplace=True)

    return latest_trade_table

def fillna_half_hour(latest_trade_table):
    """
    Description: fill with na if one the half hour is missing.
    In order to flow you need to have all the half hour for the hour to be valid
    """

    group = latest_trade_table.groupby(latest_trade_table.index.hour)['price_GB_Half_Hour_Power']
    latest_trade_table['price_GB_Half_Hour_Power'] = group.transform(lambda x: np.nan if x.isna().any() else x.mean())

    return latest_trade_table

def process_data(from_utc, to_utc, last_gb_trade, conti_trade, config):

    products = config["intraday_trades"]["GB"]["intraday_products_gb"]
    # find latest trade gb
    # find latest trade conti
    latest_trade_table = get_gb_latest_trade(last_gb_trade, from_utc, to_utc)
    latest_xbid_trade = get_conti_latest_trade(conti_trade, config)

    latest_trade_table = latest_trade_table.join(latest_xbid_trade)
    col = ["BE", "FR", "NL"]

    latest_trade_table.index.names = ["Date CET"]
    latest_trade_table = fillna_half_hour(latest_trade_table)

    latest_trade_table = latest_trade_table.resample("1H").mean()
    latest_trade_table = get_uk_product_label(latest_trade_table)

    for pattern in products:
        # Check if a column exists that contains the pattern
        matching_columns = [col for col in latest_trade_table.columns if pattern in col]

        # If no matching columns, create one and fill with zeros
        if not matching_columns:
            latest_trade_table["price_"+ pattern] = np.nan
            latest_trade_table["quantity_"+ pattern] = np.nan
    
    latest_trade_table['price_GB_2_Hour_Power'] = latest_trade_table['price_GB_2_Hour_Power'].fillna(method = "ffill", limit=1) 
    latest_trade_table['price_GB_4_Hour_Power'] = latest_trade_table['price_GB_4_Hour_Power'].fillna(method="ffill", limit=3)

    cols_to_drop = latest_trade_table.filter(regex='quantity').columns
    latest_trade_table.drop(cols_to_drop, axis=1, inplace=True)

    names = {"price_GB_2_Hour_Power": "GB 2HR", "price_GB_4_Hour_Power": "GB 4HR", "price_GB_Half_Hour_Power": "GB HH"}
    latest_trade_table.rename(columns=names, inplace=True)

    #group on date as well to avoid duplicates
    latest_trade_table = create_xbid_average(latest_trade_table, col)


    return latest_trade_table

def transform_data(gb_conti_spread, conti_gb_spread, gb_conti, conti_gb):

    """
    Create the dataframe that will be used for display
    """
    imports_name = [
            ('GB 4HR', 'GBBE', 'NEMO'),
            ('GB 4HR', 'GBFR', 'IFA1'),
            ('GB 4HR', 'GBFR', 'IFA2'),
            ('GB 4HR', 'GBNL', 'BN'),
            ('GB 2HR', 'GBBE', 'NEMO'),
            ('GB 2HR', 'GBFR', 'IFA1'),
            ('GB 2HR', 'GBFR', 'IFA2'), 
            ('GB 2HR', 'GBNL', 'BN'),
            ('GB HH', 'GBBE', 'NEMO'),
            ('GB HH', 'GBFR', 'IFA1'),
            ('GB HH', 'GBFR', 'IFA2'),
            ('GB HH', 'GBNL', 'BN')]
    exports_name = [
            ('GB 4HR', "BEGB", 'NEMO'),
            ('GB 4HR', "FRGB", 'IFA1'),
            ('GB 4HR', "FRGB", 'IFA2'),
            ('GB 4HR', "NLGB", 'BN'),
            ('GB 2HR', "BEGB", 'NEMO'),
            ('GB 2HR', "FRGB", 'IFA1'),
            ('GB 2HR', "FRGB", 'IFA2'), 
            ('GB 2HR', "NLGB", 'BN'),
            ('GB HH', "BEGB", 'NEMO'),
            ('GB HH', "FRGB", 'IFA1'),
            ('GB HH', "FRGB",'IFA2'),
            ('GB HH', "NLGB", 'BN')]
    
    gb_conti = gb_conti.round(1)
    conti_gb =  conti_gb.round(1)
    gb_conti_spread = gb_conti_spread.round(1)
    conti_gb_spread =  conti_gb_spread.round(1)

    multiindex_name = ['Products', 'Direction', 'Cable']
    gb_conti = gb_conti.sort_index(axis = 1)
    multi_level_columns = pd.MultiIndex.from_tuples(imports_name, names=multiindex_name)
    gb_conti.columns = multi_level_columns

    multi_level_columns_exp = pd.MultiIndex.from_tuples(exports_name, names=multiindex_name)
    conti_gb.columns = multi_level_columns_exp

    multi_level_columns = pd.MultiIndex.from_tuples(imports_name, names=multiindex_name)
    gb_conti_spread.columns = multi_level_columns

    multi_level_columns_exp = pd.MultiIndex.from_tuples(exports_name, names=multiindex_name)
    conti_gb_spread.columns = multi_level_columns_exp

    gb_conti = gb_conti.join(gb_conti_spread.add_suffix("_spread"))
    conti_gb = conti_gb.join(conti_gb_spread.add_suffix("_spread"))


    return gb_conti_spread, conti_gb_spread, gb_conti, conti_gb


def convert_to_euro(from_utc, to_utc, flow_table, gb_products):

    close_date = from_utc - pd.Timedelta(days=1)

    try:
        fx = get_fx(close_date, to_utc)
        flow_table[gb_products] = flow_table[gb_products]/fx[-1]
    except:
        fx = get_eur_to_gbp()
        flow_table[gb_products] = flow_table[gb_products]/fx

    return flow_table

def close_blocks(spread):
    """"
    Close the whole block after gate closure
    """

    spread = spread.dropna(how = "all")

    col_names_2hr = list(spread.filter(regex = '2HR').columns)
    current_block_2hr = spread.index.get_level_values(2)[0]
    unique_blocks = spread.index.get_level_values(2).value_counts()
    if unique_blocks[current_block_2hr] == 1:
        spread.loc[spread.index[0], col_names_2hr[0]] = np.nan
        spread.loc[spread.index[0], col_names_2hr[1]] = np.nan
        spread.loc[spread.index[0], col_names_2hr[2]] = np.nan
        spread.loc[spread.index[0], col_names_2hr[3]] = np.nan

    col_names_4hr = list(spread.filter(regex = '4HR').columns)
    current_block_4hr = spread.index.get_level_values(1)[0]
    unique_blocks = spread.index.get_level_values(1).value_counts()
    if unique_blocks[current_block_4hr] < 4:

        index = unique_blocks[current_block_4hr]
        spread.loc[spread.index[:index],col_names_4hr[0]] = np.nan
        spread.loc[spread.index[:index],col_names_4hr[1]] = np.nan
        spread.loc[spread.index[:index],col_names_4hr[2]] = np.nan
        spread.loc[spread.index[:index],col_names_4hr[3]] = np.nan

    return spread

def apply_gate_closure(spread, config):
    """
    filter time to delivery is greater than  gate closure time
    filter block 4 h, 2 h after gate closure time
    """
    gate_closure = config["gate_closure"]
    now_cet = pd.Timestamp.utcnow().tz_convert("Europe/Paris")
    spread['time_to_delivery'] = spread.index.get_level_values(0).floor('H') - now_cet

    be_gate_closure = spread['time_to_delivery'] <= pd.Timedelta(gate_closure['NEMO'])
    nl_gate_closure = spread['time_to_delivery'] <=  pd.Timedelta(gate_closure['BN'])
    fr1_gate_closure = spread['time_to_delivery'] <=  pd.Timedelta(gate_closure['IFA1'])
    fr2_gate_closure = spread['time_to_delivery'] <=  pd.Timedelta(gate_closure['IFA2'])

    column_names_nemo = spread.columns[spread.columns.get_level_values('Cable') == 'NEMO']
    column_names_bn = spread.columns[spread.columns.get_level_values('Cable') == 'BN']
    column_names_ifa1 = spread.columns[spread.columns.get_level_values('Cable') == 'IFA1']
    column_names_ifa2 = spread.columns[spread.columns.get_level_values('Cable') == 'IFA2']


    spread.loc[be_gate_closure, column_names_nemo] = np.nan
    spread.loc[nl_gate_closure, column_names_bn] =  np.nan
    spread.loc[fr1_gate_closure, column_names_ifa1] =  np.nan
    spread.loc[fr2_gate_closure, column_names_ifa2]  =  np.nan

    spread = spread.drop(columns = ('time_to_delivery', '', ''))

    spread = close_blocks(spread)

    return spread

def calculate_spread(from_utc, to_utc, flow_table, config):
    """
    Calculate real time spread for Imports and Exports:

        Imports : Conti XBID - GB*(1+loss_factor)
        Exports : GB*(1 - loss_factor) - Conti XBID
    
    ** Calculated for every product in gb
    ** Remove the product if is past gate closure for flowing 
    """
    loss_factor = config["flow_loss"]
    gb_products = config['intraday_trades']['GB']['intraday_products_gb_name']
    gb_conti = pd.DataFrame()
    conti_gb = pd.DataFrame()
    gb_conti_spread = pd.DataFrame()
    conti_gb_spread = pd.DataFrame()


    flow_table = convert_to_euro(from_utc, to_utc, flow_table, gb_products)
    flow_table.rename(columns={"BE": "BE_HH", "FR": "FR_HH", "NL": "NL_HH"}, inplace=True)


    for gb_col in gb_products:
        for loss_cable_i in loss_factor:


            gb_conti[f'{gb_col}  {loss_cable_i}'] = flow_table[gb_col]*(1 + loss_factor[loss_cable_i]*0.01)
            conti_gb[f'{loss_cable_i}  {gb_col}'] = flow_table[gb_col]*(1 - loss_factor[loss_cable_i]*0.01)
            conti_name = loss_cable_i.split("_")[0] + "_" + gb_col.split(" ")[1]
            gb_conti_spread[f'{gb_col}  {loss_cable_i}'] = flow_table[conti_name] - gb_conti[f'{gb_col}  {loss_cable_i}']
            conti_gb_spread[f'{loss_cable_i}  {gb_col}'] = conti_gb[f'{loss_cable_i}  {gb_col}'] - flow_table[conti_name]


    gb_conti_spread, conti_gb_spread, gb_conti, conti_gb = transform_data(gb_conti_spread, conti_gb_spread, gb_conti, conti_gb)
    gb_conti_spread = apply_gate_closure(gb_conti_spread, config)
    conti_gb_spread = apply_gate_closure(conti_gb_spread, config)

    return gb_conti_spread, conti_gb_spread

def create_plots(gb_conti, conti_gb, intraday_cap, config,                     
                    HH_imp_placeholder, 
                    H2_imp_placeholder, 
                    H4_imp_placeholder,
                    HH_exp_placeholder,
                    H2_exp_placeholder,
                    H4_exp_placeholder, 
                    placeholder_intraday_cap):
    #color palettes : https://matplotlib.org/stable/tutorials/colors/colormaps.html
    minimum_spread = config['minimum_spread']


    gb_conti = gb_conti.dropna(how='all')
    gb_conti.index = gb_conti.index.set_levels(gb_conti.index.levels[0].tz_localize(None), level=0)
    conti_gb = conti_gb.dropna(how = "all")
    conti_gb.index = conti_gb.index.set_levels(conti_gb.index.levels[0].tz_localize(None), level=0)

    gb_conti.columns = ['_'.join(col) for col in gb_conti.columns]
    gb_conti = gb_conti.fillna(0)
    conti_gb.columns = ['_'.join(col) for col in conti_gb.columns]
    conti_gb = conti_gb.fillna(0)


    ## SPLIT by name contained'
    col_names_imports = ['BE', 'FR1', 'FR2', 'NL']
    col_names_exp = ['BE', 'FR1', 'FR2', 'NL']
    hh_imp = gb_conti.filter(regex = 'HH')
    hh_imp.columns = col_names_imports
    hh_imp = hh_imp.style.format(precision=1).background_gradient(cmap='seismic_r', vmin= -60, vmax=60, axis=None)

    h2_imp = gb_conti.filter(regex = '2HR').drop_duplicates()
    h2_imp = h2_imp.reset_index(level=[1,3], drop=True)
    h2_imp.columns = col_names_imports
    h2_imp = h2_imp.style.format(precision=1).background_gradient(cmap='seismic_r', vmin= -60, vmax=60, axis=None)

    h4_imp = gb_conti.filter(regex = '4HR').drop_duplicates()
    h4_imp = h4_imp.reset_index(level=[2,3], drop=True)
    h4_imp.columns = col_names_imports
    h4_imp = h4_imp.style.format(precision=1).background_gradient(cmap='seismic_r', vmin= -60, vmax=60, axis=None)
    
    hh_exp = conti_gb.filter(regex = 'HH')
    hh_exp.columns = col_names_exp
    hh_exp = hh_exp.style.format(precision=1).background_gradient(cmap='seismic_r', vmin= -60, vmax=60, axis=None)
    
    h2_exp = conti_gb.filter(regex = '2HR').drop_duplicates()
    h2_exp = h2_exp.reset_index(level=[1,3], drop=True)
    h2_exp.columns = col_names_exp
    h2_exp = h2_exp.style.format(precision=1).background_gradient(cmap='seismic_r', vmin= -60, vmax=60, axis=None)
    
    h4_exp = conti_gb.filter(regex = '4HR').drop_duplicates()
    h4_exp = h4_exp.reset_index(level=[2,3], drop=True)
    h4_exp.columns = col_names_exp
    h4_exp = h4_exp.style.format(precision=1).background_gradient(cmap='seismic_r', vmin= -60, vmax=60, axis=None)
    ## RENAME
    ## add new placeholders 
    HH_imp_placeholder.dataframe(hh_imp) 
    H2_imp_placeholder.dataframe(h2_imp) 
    H4_imp_placeholder.dataframe(h4_imp)
    HH_exp_placeholder.dataframe(hh_exp)
    H2_exp_placeholder.dataframe(h2_exp)
    H4_exp_placeholder.dataframe(h4_exp) 

    intraday_cap.columns = intraday_cap.columns.str.replace('jao_capacity_auction_market_data_offer_capacities_', '')
    intraday_cap.columns = intraday_cap.columns.str.replace('_Intraday', '')
    intraday_cap.columns = intraday_cap.columns.str.replace('NLL_', '')

    intraday_cap.index = intraday_cap.index.tz_localize('UTC').tz_convert('Europe/Paris').tz_localize(None)
    intraday_cap.index.name = 'Date CET'
    try:
        columns = ['IF1_GB_FR', 'IF2_GB_FR', 'GB_BE', 'IF1_FR_GB', 'IF2_FR_GB', 'BE_GB']
        dictionary_names = {'IF1_GB_FR': 'GBFR1', 'IF2_GB_FR': 'GBFR2', 'GB_BE': 'GBBE', 'IF1_FR_GB': 'FR1GB', 'IF2_FR_GB':'FR2GB', 'BE_GB': 'BEGB'}
        intraday_cap = intraday_cap[columns].rename(columns=dictionary_names)

    except:
        print("missing columns")
    
    intraday_cap.sort_index(inplace=True)
    intraday_cap_styled = intraday_cap.style.format(precision=0).background_gradient(cmap='Reds_r', vmin= 0, vmax=300, axis=None)
    placeholder_intraday_cap.dataframe(intraday_cap_styled)


def flow_killer():
    """
    Limit the query TO 3 months max as trayport only allows 3 months of data 
    and faradyn grid trades only have 3 months of data
    """
    file = os.path.join(BASE_DIR, "flow_killer_config.yaml")
    with open(file, 'r') as file:
        config = yaml.safe_load(file)


    latest = pd.Timestamp.utcnow().round('H')
    end = latest + pd.Timedelta(days=1)

    # st.set_page_config(layout='wide')
    st.title('Live Flow Spreads')
    st.markdown("## Imports GB to Conti")
    st.markdown("HH")
    HH_imp_placeholder = st.empty()
    st.markdown("2H")
    H2_imp_placeholder = st.empty()
    st.markdown("4H")
    H4_imp_placeholder = st.empty()

    st.markdown("## Exports Conti to GB")
    st.markdown("HH")
    HH_exp_placeholder = st.empty()
    st.markdown("2H")
    H2_exp_placeholder = st.empty()
    st.markdown("4H")
    H4_exp_placeholder = st.empty()

    st.markdown("## Intraday Available Capacity")
    placeholder_intraday_cap = st.empty()

    while True:

        latest = pd.Timestamp.utcnow().round('H')
        end = latest + pd.Timedelta(days=1)

        last_gb_trade, last_conti_trade, available_capacity = get_data(latest, end, config)
        flow_table = process_data(latest, end, last_gb_trade, last_conti_trade, config)
        gb_conti, conti_gb = calculate_spread(latest, end, flow_table, config)

        # Update placeholders with new data
        
        create_plots(gb_conti, conti_gb, available_capacity, config, 
                    HH_imp_placeholder, 
                    H2_imp_placeholder, 
                    H4_imp_placeholder,
                    HH_exp_placeholder,
                    H2_exp_placeholder,
                    H4_exp_placeholder,
                    placeholder_intraday_cap)

        time.sleep(5)

