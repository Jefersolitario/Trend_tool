import os
import yaml 
import numpy as np
import pandas as pd
# from pages.trend_tracker.data import get_prices, get_actuals, get_vwap, get_ts_db, get_exaa_prices, get_nordpool
from contistreamlitapp.pages.trend_tracker.data import get_prices, get_actuals, get_vwap, get_ts_db, get_exaa_prices, get_nordpool
import plotly.express as px
import streamlit as st
import holidays
from datetime import  timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_prices_data(start_utc, end_utc, country, price_selection, settings):

    if price_selection in ['day_ahead', 'day_ahead NordPool', 'IDA1', 'IDA2', 'IDA3', 'HH', 'imbal', 'imbal short', 'imbal long']:
        kpi_settings = settings['kpi_settings']
        price_map = {
            'day_ahead': kpi_settings['day_ahead'].get(country),
            'day_ahead NordPool': kpi_settings['day_ahead'].get('GB_Nordpool'),
            'IDA1': kpi_settings['Intraday_auction']['IDA1'].get(country),
            'IDA2': kpi_settings['Intraday_auction']['IDA2'].get(country),
            'IDA3': kpi_settings['Intraday_auction']['IDA3'].get(country),
            'HH': kpi_settings['Intraday_auction']['HH'].get(country),
            'imbal': kpi_settings['imbal'].get(country, {}).get('imbal'),
            'imbal long': kpi_settings['imbal'].get(country, {}).get('imbal long'),
            'imbal short': kpi_settings['imbal'].get(country, {}).get('imbal short')
        }
        if price_map[price_selection] is None:
            st.error(f"price selection does not exist: {price_selection}")
            st.stop()
        id_dict = price_map[price_selection]
        
        price_data = get_ts_db(start_utc, end_utc, settings, id_dict)
        price_data.index = price_data.index.tz_localize('utc')
        price_data = price_data.rename(columns={"value": price_selection})
        price_data = price_data[price_selection]

    if price_selection == 'day_ahead EXXA':
        resolution_code = 'PT15M'
        price_data = get_exaa_prices(start_utc, end_utc, country, resolution_code)
        price_data = price_data.rename(columns={"price": "day_ahead EXXA"})
        price_data = price_data['day_ahead EXXA']
    elif price_selection == 'vwap_1h':
        duration =settings['kpi_settings']['intraday_trades']['vwap_duration']
        vwap_delivery_1h = settings['kpi_settings']['intraday_trades']['vwap_h_delivery'][0] - 60 
        price_data = get_vwap(country, duration, start_utc, end_utc, vwap_delivery_1h)
        price_data = price_data.rename(columns={"VWAP": "vwap_1h"})
        price_data = price_data['vwap_1h']
    elif price_selection == 'vwap_2h':
        product = "XBID_Hour_Power"
        duration =settings['kpi_settings']['intraday_trades']['vwap_duration']
        vwap_delivery_2h = settings['kpi_settings']['intraday_trades']['vwap_h_delivery'][1] - 60 
        price_data = get_vwap(country, product, start_utc, end_utc, vwap_delivery_2h, duration)
        price_data = price_data.rename(columns={"VWAP": "vwap_2h"})
        price_data = price_data['vwap_2h']
    elif price_selection == 'vwap_2h_vol':
        product = "XBID_Hour_Power"
        duration =settings['kpi_settings']['intraday_trades']['vwap_duration']
        vwap_delivery_2h = settings['kpi_settings']['intraday_trades']['vwap_h_delivery'][1] - 60 
        price_data = get_vwap(country, product, start_utc, end_utc, vwap_delivery_2h, duration)
        price_data = price_data.rename(columns={"vwap volume": "vwap_2h_vol"})
        price_data = price_data['vwap_2h_vol']
    elif price_selection == 'q_xbid_vwap_last_hour':
        product = "XBID_Quarter_Hour_Power"
        duration = 15 ## Takes the whole session
        time_before_delivery = None
        price_data = get_vwap(country, product, start_utc, end_utc, time_before_delivery, duration)
        price_data = price_data.rename(columns={"VWAP": "q_xbid_vwap_last_hour"})
        lead_times_l = [3600, 4500, 5400, 6300]
        price_data = price_data.loc[price_data['LeadTimeSeconds'].isin(lead_times_l)]

        groups = price_data.groupby('TradeEndUTC')
        volume = groups['vwap volume'].sum()
        price_data = groups.apply(lambda x: np.average(x['q_xbid_vwap_last_hour'], weights=x['vwap volume'])).reset_index(name='vwap')
        price_data['volume'] = volume.values
        price_data['datetime_cet'] = price_data['TradeEndUTC'].dt.tz_convert('Europe/Paris') - pd.Timedelta(minutes= 15)
        price_data = price_data.rename({'vwap': 'q_xbid_vwap_last_hour'}, axis= 1)
        price_data.set_index('datetime_cet', inplace= True)
        # price_data.loc[price_data['volume'] <= 15, 'q_xbid_vwap_last_hour'] = np.nan
        price_data = price_data['q_xbid_vwap_last_hour']

    elif price_selection == "2H Block vwap total":
        product = "2H Block vwap"
        duration = 60
        time_before_delivery = None
        # Calculate VWAPS
        date_start = start_utc - pd.Timedelta(hours= 4)
        date_end = end_utc + pd.Timedelta(hours=4)
        intraday_trades = get_vwap('GB', product, date_start, date_end, time_before_delivery, duration)
        intraday_trades = intraday_trades.rename(columns={"VWAP": price_selection})
        intraday_trades = intraday_trades.reset_index()
        groups = intraday_trades.groupby(['datetime_cet'])
        price_data = groups.apply(lambda x: np.average(x[price_selection], weights=x['vwap volume'])).reset_index(name='vwap')
        price_data['vol'] = intraday_trades.groupby(['datetime_cet'])['vwap volume'].sum().values
        price_data.set_index('datetime_cet', inplace= True)
        price_data = price_data.resample('1h').ffill()
        price_data = price_data.rename(columns={"vwap": price_selection})
        price_data = price_data[price_selection]
    
    elif price_selection == "gb_hh_intraday_last_hour":
        product = "HH vwap"
        duration = 60
        country = price_selection[:2].upper()
        vwap_delivery_2h = None
        date_start = start_utc - pd.Timedelta(hours= 2)
        price_data = get_vwap(country, product, date_start,  end_utc, vwap_delivery_2h, duration)
        price_data = price_data.sort_values(by = ['DeliveryEndUTC', 'TradeStartUTC'])
        price_data = price_data.drop_duplicates(subset = 'DeliveryEndUTC', keep = 'last')
        
        price_data = price_data.rename(columns={"VWAP": price_selection})
        price_data = price_data[price_selection]


    elif price_selection == "2H Block vwap last hour":
        product = "2H Block vwap"
        duration = 60
        time_before_delivery = 60

        date_start = start_utc - pd.Timedelta(hours= 4)
        date_end = end_utc + pd.Timedelta(hours=4)
        price_data = get_vwap('GB', product, date_start, date_end, time_before_delivery, duration)
        price_data = price_data.rename(columns={"VWAP": price_selection})
        price_data = price_data.resample('1h').ffill()
        price_data = price_data.rename(columns={"vwap": price_selection})
        price_data = price_data[price_selection]
    elif price_selection == "4H Block vwap last hour":
        product = "4H Block vwap"
        duration = 60
        time_before_delivery = 60

        date_start = start_utc - pd.Timedelta(hours= 4)
        date_end = end_utc + pd.Timedelta(hours=4)
        price_data = get_vwap('GB', product, date_start, date_end, time_before_delivery, duration)
        price_data = price_data.rename(columns={"VWAP": price_selection})
        price_data = price_data.resample('1h').ffill()
        price_data = price_data.rename(columns={"vwap": price_selection})
        price_data = price_data[price_selection]


    return price_data

def get_freq(exit_price_data, entry_price_data):

    if entry_price_data.index.freq != None:
        freq_entry = f"{entry_price_data.index.freq.n}min"
    else:
        freq_entry = pd.infer_freq(entry_price_data.index.sort_values())
    
    if exit_price_data.index.freq != None:
        freq_exit = f"{exit_price_data.index.freq.n}min"
    else:
        freq_exit = pd.infer_freq(exit_price_data.index.sort_values())
        if freq_exit == None:
            end_row = round(len(exit_price_data)*0.5)
            freq_exit = pd.infer_freq(exit_price_data[0:end_row].index.sort_values())

    if entry_price_data.shape[0] > exit_price_data.shape[0]:
        min_freq =  freq_entry
    else:
        min_freq =  freq_exit

    return min_freq, freq_entry, freq_exit

@st.cache_data
def get_data(start_date, end_date, country, entry_price, exit_price, settings):
    """
    Get data for trends according to the selection of the user
    """

    start_utc = start_date.tz_localize('Europe/Paris').tz_convert('UTC')
    end_utc = end_date.tz_localize('Europe/Paris').tz_convert('UTC')

    entry_price_data = get_prices_data(start_utc, end_utc, country, entry_price, settings)
    freq_entry = pd.infer_freq(entry_price_data.index.sort_values())


    if 'vol' in entry_price:
        data = pd.DataFrame(index= pd.date_range(start_utc, end_utc, freq=freq_entry))
        data = data.join([entry_price_data.tz_convert('utc')])
        # data.metadata = {'freq_entry': freq_entry}
        settings['freq_entry'] = freq_entry
    else:
        exit_price_data = get_prices_data(start_utc, end_utc, country, exit_price, settings)

        min_freq, freq_entry, freq_exit = get_freq(exit_price_data, entry_price_data)
        data = pd.DataFrame(index= pd.date_range(start_utc, end_utc, freq=min_freq))
        data = data.join([entry_price_data, exit_price_data])
        # data.metadata = {'freq_entry': freq_entry, 'freq_exit': freq_exit}
        settings['freq_entry'] = freq_entry
        settings['freq_exit'] = freq_exit
        
    
    
    return data, settings

def filter_holidays(data, holidays, country):
    """Filters out holiday data."""
    if any("holiday" in word for word in holidays):
        data = get_holidays(holidays, data, country)
        if data.empty:
            st.text('No Holidays found for the selected period')
    return data

def filter_by_days(data, days):
    """Filters data by specified days."""
    if days != 'all_days':
        data = data.loc[data.index.tz_convert('Europe/Paris').day_name().isin(days)]
    return data

def freq_to_minutes(freq):
    if 'min' in freq:
        return int(freq.replace('min', ''))  # Convert minutes to integer
    elif 'h' in freq:
        quantity = freq.replace('h', '')
        return int(quantity) * 60 if quantity else 60  # Handle empty quantity as 1 hour
    else:
        raise ValueError("Unsupported frequency format")

def resample_fill_data(data, target_freq, entry_price, freq_entry, freq_exit):

    """Resamples data to target frequency and forward fills if necessary."""
    # Convert frequencies to minutes
    entry_freq_minutes = freq_to_minutes(freq_entry)
    exit_freq_minutes = freq_to_minutes(freq_exit)

    if entry_freq_minutes > exit_freq_minutes:
        limit_fill = (entry_freq_minutes/exit_freq_minutes) - 1
        data[entry_price] = data[entry_price].ffill(limit=int(limit_fill))
    
    data = data.resample(target_freq).mean()
    
    return data


def process_data(data, entry_price, exit_price, days, holidays, country, settings):
    """
    Processes data by filtering holidays, filtering by days, resampling data,
    and forward filling if necessary.
    """

    freq_entry = settings['freq_entry']
    freq_exit = settings['freq_exit']
    
    if settings['aggregation'] == None:
        target_freq = freq_exit
    else:
        target_freq = str(settings['aggregation']) + 'min'

    data = filter_holidays(data, holidays, country)
    data = filter_by_days(data, days)
    data.index = data.index.tz_convert('Europe/Paris') ## BUGs FIX This is necessary for resample accurately
    data = resample_fill_data(data, target_freq, entry_price, freq_entry, freq_exit)

    return data

def calc_spreads(data, entry_price, exit_price):


    if exit_price == 'imbal short':
        data['spread'] = data[exit_price] - data[entry_price]
        data['spread'] = data['spread']*-1
    elif 'vol' in entry_price:
        data['spread'] = data[entry_price]
    else:
        data['spread'] = data[exit_price] - data[entry_price]

    return data

def get_holidays(holidays_selction, data, country):

    years = data.index.year.unique()

    country_holiday = []
    holidays_next_day = []
    holidays_prev_day = []

    if 'holidays' in holidays_selction:
        country_holiday = [date for year in years for date in holidays.CountryHoliday(country, years=year).keys()]

    if 'holidays_next_day' in holidays_selction:
        holiday_temp = [date for year in years for date in holidays.CountryHoliday(country, years=year).keys()]
        holidays_next_day = [date + timedelta(days=1) for date in holiday_temp]
    
    if 'holidays_previous_day' in holidays_selction:
        holiday_temp = [date for year in years for date in holidays.CountryHoliday(country, years=year).keys()]
        holidays_prev_day = [date - timedelta(days=1) for date in holiday_temp]
    
    all_holidays  = country_holiday + holidays_next_day + holidays_prev_day

    data['date'] = data.index.tz_convert('Europe/Paris').date
    holidays_df = data.loc[data['date'].isin(all_holidays)]
    holidays_df = holidays_df.drop(['date'], axis = 1)

    return holidays_df

def calc_trends(data, entry_price, exit_price):

    # data = data.dropna(subset = [entry_price, exit_price])
    data['Period'] = data.groupby(data.index.date).cumcount() + 1

    data['spread cumpnl hourly'] = data.groupby('Period')['spread'].cumsum()

    if exit_price == 'q_xbid_vwap_last_hour' or entry_price == 'q_xbid_vwap_last_hour': 
        data['spread cumpnl hourly'] = data['spread cumpnl hourly']*0.25
    
    data['name_day'] = data.index.day_name()

    return data

def create_table(data):

    
    data['date'] = pd.to_datetime(data.index.date)
    data_table = data.pivot_table(index=['date'], columns=['Period'], values=['spread'], dropna=False)
    data_table = data_table.round(0)
    data_table = data_table.dropna(how = 'all') # bug fix in table

    return data_table

def create_st_plot(trends, trend_table, entry_price):


    index_download =  trend_table.index
    trend_table.index = trend_table.index.strftime('%a-%d-%b')

    col_number = trend_table.shape[1] + 1
    trend_table.columns = list(range(1,col_number))
    if 'vol' in entry_price:
        min_vol = trends[entry_price].quantile(0.1)
        max_vol = trends[entry_price].quantile(0.9)
        fig = px.imshow(trend_table, color_continuous_scale=[[0, "red"], [1, "green"]], zmin= min_vol, zmax= max_vol ,text_auto=True)
    else:
        fig = px.imshow(trend_table, color_continuous_scale=[[0, "red"], [1, "green"]], zmin= -20, zmax= 20 ,text_auto=True)

    fig.update_layout(coloraxis_showscale=False)
    fig.update_xaxes(nticks=len(trend_table.columns))
    fig.update_xaxes(tickmode='linear', dtick=1)

    trends = trends.dropna(subset = 'spread')
    fig_short = px.line(trends, x=trends.index, y='spread cumpnl hourly', color= 'Period', title='Trend by hour')
    st.plotly_chart(fig, use_container_width=True)

    trend_table.index = index_download
    csv = trend_table.to_csv(index=True)
    b_csv = csv.encode()
    st.download_button(label="Download data as CSV", data=b_csv, file_name="trend_hourly.csv", mime="text/csv")

    st.plotly_chart(fig_short, use_container_width=True)


def trend_tracker_main():

    file = os.path.join(BASE_DIR, "config_trend.yaml")

    with open(file, 'r') as stream:
        settings = yaml.safe_load(stream)

    # st.set_page_config(layout='wide')
    st.title('Trend and Performance Analysis')
    end_date = pd.to_datetime('today') - pd.Timedelta(days=1)
    start_date = end_date - pd.Timedelta(days=13)

    # defautla fr 0 long 3 
    country = st.selectbox('Select a country', settings['kpi_settings']['country_list'], index= 0)
    weekdays = st.multiselect('Select weekday', options= settings['kpi_settings']['weekdays'], default= settings['kpi_settings']['weekdays'])
    holidays = st.multiselect('Select holidays', options= settings['kpi_settings']['holidays'], default= 'all_days')
    entry_price = st.selectbox('Select an entry price', settings['kpi_settings']['price'], index=0)

    if 'vol' in entry_price:
        exit_price = 'none'
    else:
        exit_price = st.selectbox('Select an exit price', settings['kpi_settings']['price'], index= 3) #9
    settings['aggregation'] = st.selectbox('Select an aggregation min', [None, 15,30,60,120, 240], index=0) 
    
    start_date, end_date = st.date_input('Select a date range', value=[start_date, end_date])
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)  +  pd.Timedelta(days=1)

    data, settings = get_data(start_date, end_date, country, entry_price, exit_price, settings)
    data = process_data(data, entry_price, exit_price, weekdays, holidays, country, settings)
    data = calc_spreads(data, entry_price, exit_price)
    trends = calc_trends(data, entry_price, exit_price)
    trend_table = create_table(data)

    create_st_plot(trends, trend_table, entry_price)
    
    return 

trend_tracker_main()