import certifi
import logging
from datetime import timedelta, datetime
import pandas as pd
from sheeze.date_util import BERLIN_TIMEZONE, UTC_TIMEZONE, LONDON_TIMEZONE
from pymongo import MongoClient
from itertools import product
from bson import json_util

logger = logging.getLogger(f"contibackend.{__name__}")

CROSS_BORDER_COUNTRY = ["NL", "DE", "BE", "NO", "DK", "FR", "ES", "AT"]

client =   MongoClient(
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


def get_delivery_period_from_product(product, day):
    if "H" and "Q" in product:
        hour, quarter = product.split("Q")
        hour = int(hour[1:])
        quarter = int(quarter)
        delivery_start = BERLIN_TIMEZONE.localize(
            (datetime.combine(day, datetime.min.time()) + timedelta(hours=hour - 1))
        ).astimezone(UTC_TIMEZONE)
        delivery_start = delivery_start + timedelta(minutes=(15 * (quarter - 1)))
        delivery_end = delivery_start + timedelta(minutes=15)

    elif "H" in product:
        hour = int(product[1:])
        delivery_start = BERLIN_TIMEZONE.localize(
            (datetime.combine(day, datetime.min.time()) + timedelta(hours=hour - 1))
        ).astimezone(UTC_TIMEZONE)
        delivery_end = delivery_start + timedelta(hours=1)
    else:
        hour = int(product[2:]) / 2
        delivery_start = LONDON_TIMEZONE.localize(
            (datetime.combine(day, datetime.min.time()) + timedelta(hours=hour - 0.5))
        ).astimezone(UTC_TIMEZONE)
        delivery_end = delivery_start + timedelta(hours=0.5)
    return delivery_start, delivery_end


def load_public_trades(country, day, product):
    if product:
        delivery_start, delivery_end = get_delivery_period_from_product(product, day)
    query = {
        "delivery_start": delivery_start,
        "delivery_end": delivery_end,
        "$or": [
            {
                "sell_delivery_area": {"$regex": country},
                "buy_delivery_area": {"$ne": {"$regex": country}},
            },
            {
                "buy_delivery_area": {"$regex": country},
                "sell_delivery_area": {"$ne": {"$regex": country}},
            },
        ],
    }

    collection = client["EpexSpot"]["AutotraderPublicTrades"]

    fields = {
        "trade_id": 1,
        "buy_delivery_area": 1,
        "sell_delivery_area": 1,
        "delivery_start": 1,
        "delivery_end": 1,
        "execution_time": 1,
        "price": 1,
        "quantity": 1,
    }
    trades = list(collection.find(query, fields).sort("execution_time", 1))

    trade_data = pd.DataFrame(trades)

    if trade_data.empty:
        return trade_data
    trade_data.rename(
        columns={
            "delivery_start": "StartTimeUTC",
            "delivery_end": "EndTimeUTC",
            "execution_time": "ExecutionTimeUTC",
            "price": "Price",
            "quantity": "Volume",
        },
        inplace=True,
    )

    trade_data.drop_duplicates(
        subset=["trade_id", "buy_delivery_area", "sell_delivery_area", "StartTimeUTC"],
        inplace=True,
    )
    trade_data["ExecutionTimeCET"] = trade_data["ExecutionTimeUTC"].dt.tz_convert(
        BERLIN_TIMEZONE
    )

    trade_data["Product"] = product

    return trade_data


def cross_border_data(
    ref_country: str, to_country: str, date: str, hour: str, frequency: str
) -> pd.DataFrame:
    date = pd.to_datetime(date)
    public_trades = load_public_trades(ref_country, date.date(), hour)
    if public_trades.empty:
        return public_trades
    if to_country == "All":
        sell_cross = public_trades[
            ~public_trades["buy_delivery_area"].str.contains(ref_country)
        ]
        buy_cross = public_trades[
            ~public_trades["sell_delivery_area"].str.contains(ref_country)
        ]
    else:
        sell_cross = public_trades[
            public_trades["buy_delivery_area"].str.contains(to_country)
        ]
        buy_cross = public_trades[
            public_trades["sell_delivery_area"].str.contains(to_country)
        ]
    if sell_cross.empty:
        cross_trades_grouped = buy_cross.groupby(
            pd.Grouper(freq=frequency, key="ExecutionTimeCET")
        ).sum()
        cross_trades_grouped.rename(columns={"Volume": "BuyVolume"}, inplace=True)
        cross_trades_grouped["SellVolume"] = 0
    elif buy_cross.empty:
        cross_trades_grouped = sell_cross.groupby(
            pd.Grouper(freq=frequency, key="ExecutionTimeCET")
        ).sum()
        cross_trades_grouped.rename(columns={"Volume": "SellVolume"}, inplace=True)
        cross_trades_grouped["BuyVolume"] = 0
    elif buy_cross.empty & sell_cross.empty:
        print("No cross border trades")
    else:
        buy_cross_grouped = buy_cross.groupby(
            pd.Grouper(freq=frequency, key="ExecutionTimeCET")
        ).sum()
        buy_cross_grouped.rename(columns={"Volume": "BuyVolume"}, inplace=True)
        sell_cross_grouped = sell_cross.groupby(
            pd.Grouper(freq=frequency, key="ExecutionTimeCET")
        ).sum()
        sell_cross_grouped.rename(columns={"Volume": "SellVolume"}, inplace=True)
        cross_trades_grouped = pd.merge(
            buy_cross_grouped["BuyVolume"],
            sell_cross_grouped["SellVolume"],
            how="outer",
            left_index=True,
            right_index=True,
        )
        cross_trades_grouped.fillna(0, inplace=True)
    cross_trades_grouped["SellVolume"] = -cross_trades_grouped["SellVolume"]
    cross_trades_grouped["AccumVolume"] = (
        cross_trades_grouped["BuyVolume"] + cross_trades_grouped["SellVolume"]
    ).cumsum()
    cross_trades_grouped['Startdate CET'] = public_trades['StartTimeUTC'].unique()[0]
    cross_trades_grouped['Startdate CET'] = cross_trades_grouped['Startdate CET'].dt.tz_convert('Europe/Berlin')

    return cross_trades_grouped.reset_index()


def get_id_flow_xbid_evolution(start, end, country, freq):

    xbid_flow_evol_df = pd.DataFrame()
    errors = []
    dates = pd.date_range(start, end)
    hours = range(1, 25)

    for date, hour in product(dates, hours):

        try:
            xbid_flow_evol = cross_border_data(country, "All", date, "H" + str(hour), freq)
            xbid_flow_evol_df = xbid_flow_evol_df.append(xbid_flow_evol)
        except Exception as e:
            print(e)
            errors.append(f"Error on {date} hour {hour}: {e}")

    xbid_flow_evol_df['Country'] = country
    xbid_flow_evol_df['frequency'] = freq
    xbid_flow_evol_df['Product'] = "XBID_Hour_Power"

    return xbid_flow_evol_df

def save_to_db(xbid_flow_evol_hist):

    xbid_flow_evol_hist['ExecutionTimeCET'] = pd.to_datetime(xbid_flow_evol_hist['ExecutionTimeCET'])
    xbid_flow_evol_hist['ExecutionTimeUTC'] = xbid_flow_evol_hist['ExecutionTimeCET'].dt.tz_convert('UTC')
    xbid_flow_evol_hist['StartTimeUTC'] = xbid_flow_evol_hist['Startdate CET'].dt.tz_convert('UTC')
    xbid_flow_evol_hist['lead time'] = xbid_flow_evol_hist['StartTimeUTC'] - xbid_flow_evol_hist['ExecutionTimeUTC']
    xbid_flow_evol_hist['lead time'] = xbid_flow_evol_hist['lead time'].dt.total_seconds()/60
    xbid_flow_evol_hist.drop(['ExecutionTimeCET', 'Startdate CET'], axis=1, inplace= True)
    xbid_flow_evol_hist['frequency'] = xbid_flow_evol_hist['frequency'].str.extract('(\d+)').astype(float)
    xbid_flow_evol_hist.drop('Price', axis = 1, inplace = True)

    collection = client['testDB']['xbid_flows_evolution']
    data_dict = xbid_flow_evol_hist.to_dict(orient='records')
    collection.insert_many(data_dict)



# xbid_flow_evol_hist = get_id_flow_xbid_evolution('2023-11-29', '2023-12-03', "NL", "15min")
# save_to_db(xbid_flow_evol_hist)

