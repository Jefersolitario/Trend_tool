import os
import yaml
import certifi
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import streamlit as st

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

def get_data(start, end):

    collection = client["EEX"]["Outages_Power"]

    filter_query = {
    "Source": {"$in": ["Solar", "Wind Offshore", "Wind Onshore"]},
    "Country": {"$in": ["BE", "DE", "FR", "GB", "NL"]},
    "NUMStartDate": {"$gte": start, "$lte": end},
    "NonavailabilityReason": {"$in": ["", "External factors", "Other", "Outage"]}}

    fields = {
        "_id": 0,
        "product_type": 1,
        "delivery_start": 1,
        "execution_time": 1,
        "price": 1,
        "quantity": 1,
    }

    outages = list(collection.find(filter_query))#, fields
    outages_df = pd.DataFrame(outages)


    return outages_df


def process_data(outages_df):


    dfs = [generate_time_series(row[1]) for row in outages_df.iterrows()]
    outages_ts = pd.concat(dfs).reset_index(drop=True)
    outages_ts = outages_ts.sort_values(by = ['TimeStamp', 'publicationtime'])
    outages_ts.set_index('TimeStamp', inplace= True)

    return outages_ts


def generate_time_series(row):

    start = pd.to_datetime(row['NUMStartDate'])
    end = pd.to_datetime(row['NUMEndDate'])
    timestamps = pd.date_range(start=start, end=end, freq='15T')  # Assuming 15 minutes frequency
    available_capacity = float(row['InstalledCapacity'])
    num_capacity = float(row['NUMCapacity'])
    publication_time = pd.to_datetime(row['PublicationTimeStamp'])
    outage_type = row['Type']
    unit_id = row['UnitID']
    event_id = row['EventID']

    outage_ts = pd.DataFrame({
        'TimeStamp': timestamps[:-1],  # remove the last time, to have intervals
        'AvailableCapacity': available_capacity,
        'NUMCapacity': num_capacity,
        'publicationtime': publication_time,
        'type': outage_type,
        'UnitID': unit_id,
        'EventID': event_id
    })


    return outage_ts



def create_st_plot(data):

    return print('done')

def weather_tools_links():

    file = os.path.join(BASE_DIR, "config_weather.yaml")

    with open(file, 'r') as stream:
        settings = yaml.safe_load(stream)

    st.title('Weather Reports and Tools: ')

    # For Weather DAH forecast error
    st.write(
        f"[Weather DAH forecast error:]({settings['Links']['Weather DAH forecast error']})")

    # For Weather DAH forecast weekly error
    st.write(
        f"[Weather DAH forecast Weekly error:]({settings['Links']['Weather DAH Weekly error']})")

    # For Weather intraday forecast error
    st.write(
        f"[Weather intraday forecast error:]({settings['Links']['Weather intraday forecast error']}) | "
        f"[Documentation:]({settings['Links']['docs intraday forecast error']})"
    )

    # For weather dah forecast profitability
    st.write(
        f"[weather dah forecast profitability:]({settings['Links']['weather dah forecast profitability']}) | "
        f"[Documentation:]({settings['Links']['docs dah forecast profitability']})"
    )

    # For Weather forecast profitability
    st.write(f"[Weather forecast profitability:]({settings['Links']['Weather forecast profitability']})")

    # For Wind installed capacity Update
    st.write(f"[Wind installed capactiy Update:]({settings['Links']['wind installed capactiy Update']})| "
        f"[Documentation:]({settings['Links']['docs installed capactiy Update']})"
    )
    # For Generation data report
    st.write(f"[Generation data report:]({settings['Links']['generation data report']})| "
        f"[Documentation:]({settings['Links']['docs generation data report']})"
    )

    st.title('Live Curtailments data')

    # latest = pd.Timestamp.utcnow().round('H')
    # end = latest + pd.Timedelta(days=1)
    
    # data = get_data(latest, end)
    # outages_ts = process_data(data)
    # create_st_plot(outages_ts)

# weather_tools_links()