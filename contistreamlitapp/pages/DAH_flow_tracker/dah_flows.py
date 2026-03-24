import os
import yaml
import pandas as pd
import plotly.express as px
import streamlit as st
from pages.DAH_flow_tracker.data import get_ts_hot_actuals, get_prices

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_data(start, end, country, interconnector, direction, config):

    start_utc = start.tz_localize('Europe/Paris').tz_convert('UTC')
    end_utc = end.tz_localize('Europe/Paris').tz_convert('UTC')
    data_raw = pd.DataFrame(index= pd.date_range(start_utc, end_utc, freq= 'h'))

    if direction == 'imports':

        id_import = config['cap_prices'][country][interconnector]["import_id"]
        id_import_name = config['cap_prices'][country][interconnector]["import_name"]
        cap_price = get_ts_hot_actuals(start_utc, end_utc, config, id_import, id_import_name)
    else:

        id_export = config['cap_prices'][country][interconnector]["export_id"]
        id_export_name = config['cap_prices'][country][interconnector]["export_name"]
        cap_price = get_ts_hot_actuals(start_utc, end_utc, config, id_export, id_export_name)
    
    auction = config['dah'][country]['auction']
    country_spot = get_prices(start_utc, end_utc, country, auction)
    name = country + ' spot'
    country_spot = country_spot.rename(columns={"Value": name})


    auction = config['dah']['GB']['auction']
    UK_spot = get_prices(start_utc, end_utc, "GB", auction)
    name_uk = 'UK spot'
    UK_spot = UK_spot.rename(columns={"Value": name_uk})

    data_raw = data_raw.join([country_spot[name], UK_spot[name_uk], cap_price])
    
    return data_raw

def calc_spreads(data, country, interconnector, direction, config):

    losses = config['flow_loss'][interconnector]
    losses = losses/100


    if direction == 'imports':

        cap_dah_import = config['cap_prices'][country][interconnector]["import_name"]
        data['spread'] = data[country + ' spot'] - data['UK spot']*config['fx']*(1 + losses)
        data['spread'] = data['spread'] - data[cap_dah_import]

    else:
        cap_dah_export = config['cap_prices'][country][interconnector]["export_name"]
        data['spread'] = (data['UK spot']*config['fx']*(1 - losses)) - data[country + ' spot']
        data['spread'] = data['spread'] - data[cap_dah_export]

    return data

def calc_trends(data):

    # group by hour and calculate the cumulatieve pnl
    data['spread cumpnl hourly'] = data.groupby(data.index.hour)['spread'].cumsum()
    data['name_day'] = data.index.day_name()

    return data

def create_table(data):

    data.index = data.index.tz_convert('Europe/Paris')
    hour_ending = 1
    data['Period'] = data.index.hour + hour_ending
    data['date'] = pd.to_datetime(data.index.date)
    data_table = data.pivot_table(index=['date'], columns=['Period'], values=['spread'], dropna=False)
    data_table = data_table.round(0)
    data_table.index = data_table.index.strftime('%a-%d-%b')

    return data_table

def create_st_plot(trend_table, trends):


    trend_table.columns = list(range(1,25))
    fig = px.imshow(trend_table, color_continuous_scale=[[0, "red"], [1, "green"]], zmin= -20, zmax= 20 ,text_auto=True)
    fig.update_layout(coloraxis_showscale=False)
    fig.update_xaxes(nticks=len(trend_table.columns))
    fig.update_xaxes(tickmode='linear', dtick=1)
    hour_ending = trends.index.hour +1
    fig_short = px.line(trends, x=trends.index, y='spread cumpnl hourly', color= hour_ending, title='Trend by hour')
    st.plotly_chart(fig, use_container_width=True)
    st.plotly_chart(fig_short, use_container_width=True)


def dah_flows_main():

    file = os.path.join(BASE_DIR, "config.yaml")

    with open(file, 'r') as file:
        config = yaml.safe_load(file)

    end_date = pd.to_datetime('today') - pd.Timedelta(days=1)
    start_date = end_date - pd.Timedelta(days=8)
    ## select country

    st.title('DAH Flow tracker')
    country = st.selectbox('Select Country', config['country_list'])
    interconnector = st.selectbox('Select interconnector', config['interconnector_names'])
    direction = st.selectbox('Select a Direction', config['direction'])
    
    start_date, end_date = st.date_input('Select a date range', value=[start_date, end_date])
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)  +  pd.Timedelta(days=1)



    data = get_data(start_date, end_date, country, interconnector, direction, config)
    data = calc_spreads(data, country, interconnector, direction, config)

    trends = calc_trends(data)
    trend_table = create_table(data)
    create_st_plot(trend_table, trends)

