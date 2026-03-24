import os
import yaml 
import numpy as np
import pandas as pd

from pages.performance_analysis.utilities.data import get_dah_positions, get_intraday_positions, get_total_pnl, get_vwap, get_ts_db
from pages.performance_analysis.utilities.data import get_vol_based_vwap, get_vwap, get_nominated_positions, get_own_trades
from pages.performance_analysis.utilities.data import  get_intraday_positions, get_transactions, fetch_public_trades_data
from pages.performance_analysis.utilities.data import get_ladder_enappsys, get_niv_enappsys, get_midprice_enappsys
from pages.performance_analysis.utilities.data import get_strategy, get_strategy_old
from pages.performance_analysis.utilities.kpi_metrics import Kpi

# from contistreamlitapp.pages.performance_analysis.utilities.data import get_dah_positions, get_intraday_positions, get_total_pnl, get_vwap, get_ts_db
# from contistreamlitapp.pages.performance_analysis.utilities.data import get_vol_based_vwap, get_vwap, get_nominated_positions, get_own_trades
# from contistreamlitapp.pages.performance_analysis.utilities.data import  get_intraday_positions, get_transactions, fetch_public_trades_data
# from contistreamlitapp.pages.performance_analysis.utilities.data import get_ladder_enappsys, get_niv_enappsys, get_midprice_enappsys
# from contistreamlitapp.pages.performance_analysis.utilities.data import get_strategy, get_strategy_old
# from contistreamlitapp.pages.performance_analysis.utilities.kpi_metrics import Kpi


import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_data_select(start_date, end_date, selection , country, settings):
    
    kpi_settings = settings['kpi_settings']
    start_utc = start_date.tz_localize('Europe/Paris').tz_convert('utc') 
    end_utc = end_date.tz_localize('Europe/Paris').tz_convert('utc')

    if selection == 'DAH':
        portfolio = kpi_settings['DAH'][country]['portfolio']
        product = kpi_settings['DAH'][country]['product']
        trade_selection = get_dah_positions(portfolio, product, country, start_utc, end_utc)
        trade_selection.rename({'DAH_Opening_Price': 'DAH'}, axis= 1, inplace= True)

        if trade_selection.empty== True:

            date_range = pd.date_range(start_date, end_date, freq='15min', tz='UTC')
            trade_selection = pd.DataFrame(index = date_range, columns=['DAH', 'DAH Volume'])
    
    if selection == 'ID_trades':
        product_intraday = kpi_settings['intraday']['product']
        position_endpoint = kpi_settings['intraday']['position_endpoint']
        intraday_position = get_intraday_positions(country, product_intraday, start_utc, end_utc, position_endpoint)
        intraday_position.set_index('StartTimeUTC', inplace= True)
        trade_selection = intraday_position[['OpenPosition', 'BuyVWAP', 'SellVWAP']]
        trade_selection.rename({'OpenPosition': 'Volume'}, axis=1, inplace= True)

    if selection ==  'Total PnL':
        resolution =  kpi_settings['Total PnL'][country]['resolution']
        portfolio = kpi_settings['Total PnL'][country]['portfolio']
        pnl_total = get_total_pnl(country, portfolio, start_utc, end_utc, resolution, kpi_settings)
        trade_selection = pnl_total[['PnLRealized', 'OpenPosition', 'ImbalanceQty']]
        trade_selection.rename({'PnLRealized': 'Profit', 'ImbalanceQty': selection + '_Opening_Volume'}, axis= 1, inplace= True)
    
    if selection == 'ID_VWAP_2h':
        product = "XBID_Hour_Power"
        duration =settings['kpi_settings']['intraday_trades']['vwap_duration']
        vwap_delivery_2h = settings['kpi_settings']['intraday_trades']['vwap_h_delivery'][1] - 60 
        price_data = get_vwap(country, product, start_utc, end_utc, vwap_delivery_2h, duration)
        price_data = price_data.rename(columns={"VWAP": selection, "vwap volume": selection + ' vol'})
        trade_selection = price_data[selection]
    
    if selection == "2H Block vwap total":
        product = "2H Block vwap"
        duration = 60
        time_before_delivery = None
        # Calculate VWAPS
        date_start = start_utc - pd.Timedelta(hours= 4)
        date_end = end_utc + pd.Timedelta(hours=4)
        intraday_trades = get_vwap('GB', product, date_start, date_end, time_before_delivery, duration)
        intraday_trades = intraday_trades.rename(columns={"VWAP": selection})
        intraday_trades = intraday_trades.reset_index()
        groups = intraday_trades.groupby(['datetime_cet'])
        price_data = groups.apply(lambda x: np.average(x[selection], weights=x['vwap volume'])).reset_index(name='vwap')
        price_data['vol'] = intraday_trades.groupby(['datetime_cet'])['vwap volume'].sum().values
        price_data.set_index('datetime_cet', inplace= True)
        price_data = price_data.resample('1h').ffill()
        price_data = price_data.rename(columns={"vwap": selection})
        trade_selection = price_data[selection]
    if selection == "4H Block vwap total":
        product = "4H Block vwap"
        duration = 60
        time_before_delivery = None
        # Calculate VWAPS
        date_start = start_utc - pd.Timedelta(hours= 4)
        date_end = end_utc + pd.Timedelta(hours=4)
        intraday_trades = get_vwap('GB', product, date_start, date_end, time_before_delivery, duration)
        intraday_trades = intraday_trades.rename(columns={"VWAP": selection})
        intraday_trades = intraday_trades.reset_index()
        groups = intraday_trades.groupby(['datetime_cet'])
        price_data = groups.apply(lambda x: np.average(x[selection], weights=x['vwap volume'])).reset_index(name='vwap')
        price_data['vol'] = intraday_trades.groupby(['datetime_cet'])['vwap volume'].sum().values
        price_data.set_index('datetime_cet', inplace= True)
        price_data = price_data.resample('1h').ffill()
        price_data = price_data.rename(columns={"vwap": selection})
        trade_selection = price_data[selection]

    if selection == 'ID_vwap_vol':
        product = "XBID_Hour_Power"
        vol = settings['kpi_settings']['vol']
        price_data = get_vol_based_vwap(country, product, start_utc, end_utc, vol)
        trade_selection = price_data
        trade_selection.rename({'vwap': 'ID_vwap_vol', 'Volume': 'ID_Volume'}, axis=1, inplace= True)

    if selection == 'ID':
        trade_selection = 'get_trade'
    
    if selection == 'Imbalance':
        trade_selection = 'get_trade'

    return trade_selection 

@st.cache_data
def get_trades_data(start_date, end_date, country, settings):

    kpi_settings = settings['kpi_settings']
    country = settings['kpi_settings']['country']
    date_index = pd.date_range(start_date, end_date, freq= '15min', tz= 'Europe/Paris')
    data = pd.DataFrame(index = date_index)

    entry = get_data_select(start_date, end_date, kpi_settings['entry'] , country, settings)
    if entry.dropna().empty == True:
        st.write("Not trades for the selected period")
        st.stop()
    
    exit = get_data_select(start_date, end_date, kpi_settings['exit'] , country, settings)

    data = data.join([entry, exit])

    return data


def get_dummy_signal(start_date, end_date, strategy):
    """
    Short block 3 & 5 excluding weekends 
    """
    data = pd.DataFrame(index = pd.date_range(start_date, end_date, freq= 'h', tz= 'utc'))
    data['hour'] = data.index.tz_convert('Europe/London').hour
    data['weekday'] = data.index.tz_convert('Europe/London').weekday

    ## hours corresponding to block 3 & 5
    block_hours = [7,8,9,10,15,16,17,18]
    # block_hours = [15,16,17,18] # Only Block 5
    weekends = [5,6]
    signal = data.copy()

    signal.loc[signal['hour'].isin(block_hours), 'Signal'] = -1
    signal.loc[signal['weekday'].isin(weekends), 'Signal'] = np.nan
    # mask = (signal['hour'].isin(block_hours)) & (signal['weekday'].isin(weekends))
    # signal.loc[mask, 'Signal'] = -1

    signal['Signal_probability'] = 1
    signal['Volume'] = 50
    signal['CalculationTImeUTC'] =  signal.index.tz_convert('Europe/Paris').floor('D') - pd.Timedelta(hours= 16)
    signal['DeliveryDayCET'] = signal.index.tz_convert('Europe/Paris').floor('D')

    return signal

def get_strategy_db(kwargs):

    strategy = kwargs['strategy']
    start_date = kwargs['start_date']
    end_date = kwargs['end_date']

    new_schema_strat_list = ['at_solar_sky_clear', 'nl_solar_id']
    if strategy in new_schema_strat_list:
        signal = get_strategy(start_date, end_date, strategy)
    ## put another condition to read from a new source mongo db!!!!
    elif 'dummy' not in strategy:
        signal = get_strategy_old(start_date, end_date, strategy)
        if signal is None or signal.empty:
        #     st.text_area('No signal generated stopping program')
            return
    else:
        signal = get_dummy_signal(start_date, end_date, strategy)
    signal.rename({'Direction_probability': 'Probability', 'Signal': 'Signal '+ strategy}, axis= 1, inplace= True)
    signal = signal['Signal '+ strategy]

    if strategy == 'strat_france_sky_clear':
        signal = signal*-1

    return signal

@st.cache_data
def get_strategies_for_country(start_date, end_date, country, settings):

    current_dir = os.path.dirname(__file__)  # Get the current directory
    parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))  # Get the parent directory
    yaml_file_path = os.path.join(parent_dir, settings['signal_config_path'])  # Construct the full path to the YAML file

    # Read the YAML file
    with open(yaml_file_path, 'r') as file:
        yaml_content = yaml.safe_load(file)

    strategy_list = yaml_content['kpi_settings']['strategy_list']
    signals = pd.DataFrame(index= pd.date_range(start_date, end_date, freq= 'h'))

    ## filter the results on the country
    strategy_selection = {strategy: details for strategy, details in yaml_content.items() 
            if strategy in strategy_list and details.get('country') == country}
            
    ## get the signal using a loop with the reaming keys
    strategy_args = [{'strategy': strategy, 'start_date': start_date, 'end_date': end_date} for strategy in strategy_selection.keys()]
    print(strategy_args)
    signals_list = list(map(get_strategy_db, strategy_args))
    ## MIGRATE THE SIGNAL TO NEW COLLECTION JUST MODIFY THE QUERY IN MONGO DB TO QUERY MULTIPLE STRATS AT ONCE!!!!!!
    signals = pd.concat(signals_list, axis=1)

    return signals



def get_reference_price(start_utc, end_utc, country, settings):
    "Reference Prices include Spot, Imbal, Midprice"
    # get imbalance price
    
    meta_id = settings['kpi_settings']['imbal'][country]
    spot_meta_id = settings['kpi_settings']['spot'][country]

    spot = get_ts_db(start_utc, end_utc, settings, spot_meta_id, 'day ahead')
    spot.index = spot.index.tz_localize('utc')

    if country == "NL":
        mid_price = get_midprice_enappsys(start_utc, end_utc, settings['enappsys'])
    
    start_utc_ts = start_utc #- pd.Timedelta(hours= 1)
    if country in ["FR", "NL", "BE"]:
        
        imbal_short = get_ts_db(start_utc_ts, end_utc, settings, meta_id['imbal short'], 'imbal down')
        imbal_long = get_ts_db(start_utc_ts, end_utc, settings, meta_id['imbal long'], 'imbal')
        imbal = imbal_long.join(imbal_short)

        
    else:
        imbal = get_ts_db(start_utc_ts, end_utc, settings, meta_id['imbal'], 'imbal')
    
    
    imbal.index = imbal.index.tz_localize('utc')
    price_levels = imbal.join(spot)

    if country == "NL":
        price_levels = price_levels.join(mid_price['MID PRCE'])
    
    return price_levels

def add_imbal_data(intraday_data, price_levels, start_utc, end_utc, country, settings):

    if country == "NL":

        ladder = get_ladder_enappsys(start_utc, end_utc, settings['enappsys'])
        niv = get_niv_enappsys(start_utc, end_utc, settings['enappsys'])
        niv_col = ['aFRR UP', 'aFRR DOWN', "IGCC UP", "IGCC DOWN"]
        ladder_col = ['NEGATIVE PRICE (600)', 'NEGATIVE PRICE (300)', 'NEGATIVE PRICE (100)', 'POSITIVE PRICE (100)', 'POSITIVE PRICE (300)', 'POSITIVE PRICE (600)']
        price_levels = price_levels.join([ladder[ladder_col], niv[niv_col]])
        intraday_data._metadata = price_levels
    elif country == "FR":
        start_utc_ts = start_utc - pd.Timedelta(hours= 1)
        end_utc_ts = end_utc + pd.Timedelta(minutes=30)
        dates = pd.date_range(start= start_utc_ts, end= end_utc_ts, freq= '30min')
        imbal_vol = pd.DataFrame(index = dates)
        imbal_vol_ids = settings['kpi_settings']['imbal'][country]['imbal_niv']

        for name_i, metaid_i in imbal_vol_ids.items():

            temp_data = get_ts_db(start_utc_ts, end_utc, settings, metaid_i, name_i)
            temp_data.index = temp_data.index.tz_localize('utc')
            imbal_vol = imbal_vol.join(temp_data, how='left')
        price_levels = price_levels.join(imbal_vol)
        # price_levels = imbal_vol.join(price_levels)
        intraday_data._metadata = price_levels
    elif country == "AT":
        intraday_data._metadata = price_levels
    else:
        intraday_data._metadata = price_levels
    
    ### Add own imbal trades
    kpi_settings = settings['kpi_settings']
    resolution =  kpi_settings['Total PnL'][country]['resolution']
    portfolio = kpi_settings['Total PnL'][country]['portfolio']
    try:
        end_utc_t = end_utc + pd.Timedelta(days= 1)
        imbal_vol = get_total_pnl(country, portfolio, start_utc, end_utc_t, 'HH', kpi_settings)
        imbal_vol = imbal_vol.loc[imbal_vol.index.hour == start_utc.hour, 'ImbalanceQty']
        price_levels = price_levels.join(imbal_vol)
        intraday_data._metadata = price_levels
    except:
        print('No Imbalance trades left')

    return intraday_data

def get_cwe_public_trades(public_trades, start_utc, end_utc, cwe_public_trades):

    for country in cwe_public_trades: 
        public_trades_cwe = get_transactions(start_utc, end_utc,country, ["XBID_Hour_Power", "Intraday_Hour_Power"])
        public_trades_cwe.set_index('StartTimeUTC', inplace= True)
        public_trades_cwe['TradingPortfolio'] = 'Public_' + country
        public_trades_cwe = public_trades_cwe[['Price', 'VolumeMW', 'ExecutionTimeUTC','TradingPortfolio']]
        public_trades = pd.concat([public_trades, public_trades_cwe])

    return public_trades

def get_intraday_trades(start,  product, show_portfolio, cwe_public_trades, settings):

    start_utc = start.tz_localize('Europe/Paris').tz_convert('utc')
    intraday_data = pd.DataFrame() 
    end_utc = start_utc + pd.Timedelta(minutes= 45)
    country = settings['kpi_settings']['country']
    portfolio = settings['kpi_settings']['intraday_trades'][country]["portfolio"]

    # remove empty hours
    if country == "GB" and 'GB_Half_Hour_Power' not in product:
        if "GB_2_Hour_Power" in product and start.hour % 2 == 1:
            st.write("2 Hour block hour does not exist, 2 block starts with even hours")
            st.stop()
        if "GB_4_Hour_Power" in product and start.hour % 4 != 0:
            st.write("4 hour block does not exist 4 block starts with 4 hour blocks")
            st.stop()

    # get public trades
    # public_trades = get_transactions(start_utc, end_utc, country, product)
    start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    public_trades = fetch_public_trades_data(product, country, start_str, end_str, settings)
    public_trades = public_trades.rename({'price':'Price', 'quantity': 'VolumeMW', 'timestampUtc': 'ExecutionTimeUTC', 'listedInstrument_deliveryDateRangeUtc_startTs': 'StartTimeUTC'}, axis= 1) 

    if len(public_trades) == 0:
        st.write('No Public trades Found in the db')
        st.stop()

    public_trades.set_index('StartTimeUTC', inplace= True)
    public_trades['TradingPortfolio'] = 'Public'
    public_trades = public_trades[['Price', 'VolumeMW', 'ExecutionTimeUTC','TradingPortfolio']]

    if len(cwe_public_trades) > 0:
        public_trades = get_cwe_public_trades(public_trades, start_utc, end_utc, cwe_public_trades)
    # get own trades
    try:
        own_trades = get_own_trades(start_utc, end_utc, country, product, portfolio)
        own_trades['Direction'] = own_trades['Side'].map({"B": 1, "S": -1})
        own_trades['VolumeMW'] = own_trades['VolumeMW']*own_trades['Direction']
        own_trades.set_index('StartTimeUTC', inplace= True)

        if show_portfolio == False:
            own_trades['TradingPortfolio'] = 'Conti' #Hide portfolio Name
        own_trades = own_trades[['Price', 'VolumeMW', 'ExecutionTimeUTC', 'TradingPortfolio']]
    except:
        st.write("Not trades found displaying public trades only")
        dates = pd.date_range(start= start_utc, end= end_utc, freq= 'h')
        own_trades = pd.DataFrame(index = dates)
    
    intraday_data = pd.concat([public_trades, own_trades])
    intraday_data = intraday_data.dropna(how = 'all')
    intraday_data.index = pd.to_datetime(intraday_data.index)


    price_levels = get_reference_price(start_utc, end_utc, country, settings)
    ### Add imbal data !!! 
    intraday_data = add_imbal_data(intraday_data, price_levels, start_utc, end_utc, country, settings)
    

    return intraday_data


def get_country_nomination(start_date, end_date, country, settings):

    if country in ["NL", "BE", "FR"]:

        nomination = get_nominated_positions(start_date, end_date, country, "Intraday", settings)

    if country == "FR":
        ### get IFA1 and IFA2 nominations and aggregate 
        settings["flow_nomination"]["cables"]["FR"] = "IFA2"
        nomination_IFA2 = get_nominated_positions(start_date, end_date, country, "Intraday", settings)
        nomination['GBFRNom'] = nomination['GBFRNom'] + nomination_IFA2['GBFRNom']
        nomination['FRGBNom'] = nomination['FRGBNom'] + nomination_IFA2['FRGBNom']
    if country == "GB":
        ### get all flow nominations and aggregate
        countries = ["FR", "NL", "BE"]
        index_range = pd.date_range(start_date, end_date, freq= "h")
        nomination = pd.DataFrame(index = index_range, columns = [ "GBConti", "ContiGB", "Cable"])
        nomination.index.name = "StartTimeUTC"
        nomination.reset_index(inplace= True)
        nomination[["GBConti", "ContiGB"]] = 0
        nomination['Cable'] = "All"

        for country_i in countries:
            nomination_i = get_nominated_positions(start_date, end_date, country_i, "Intraday", settings)
            nomination['GBConti'] = nomination['GBConti'] + nomination_i.iloc[:, 1]
            nomination['ContiGB'] = nomination['ContiGB'] + nomination_i.iloc[:, 2]
        
        settings["flow_nomination"]["cables"]["FR"] = "IFA2"
        nomination_IFA2 = get_nominated_positions(start_date, end_date, "FR", "Intraday", settings)
        nomination['GBConti'] = nomination['GBConti'] + nomination_IFA2['GBFRNom']
        nomination['ContiGB'] = nomination['ContiGB'] + nomination_IFA2['FRGBNom']
    if country == "AT":
        nomination = pd.DataFrame(index= pd.date_range(start_date, end_date, freq= 'h'))
        nomination.index.name = 'StartTimeUTC'
        # nomination.reset_index(inplace= True)



    return nomination

def calc_pnl(data, settings):

    kpi_settings = settings['kpi_settings']
    entry = kpi_settings['entry']
    exit = kpi_settings['exit']
    fee = kpi_settings[entry]['fee'] + kpi_settings[exit]['fee']

    if 'Profit' not in data.columns:

        data['Spread'] = data[exit] - data[entry]
        data['Profit'] = data['Spread']*data[entry + '_Opening_Volume']
        data['Profit'] = data['Profit'] - data[entry + '_Opening_Volume']*fee

    data['day_name'] = data.index.tz_convert('Europe/Amsterdam').day_name()
    data = data.loc[data['day_name'].isin(settings['kpi_settings']['weekdays_sel'])]   
    data["cum_pnl"] = data["Profit"].cumsum()


    return data

def calculate_metrics(pnl_trades, settings):

    pnl_data = pnl_trades.copy()
    kpi_settings = settings['kpi_settings']
    entry = kpi_settings['entry']
    exit = kpi_settings['exit']
    fee = kpi_settings[entry]['fee'] + kpi_settings[exit]['fee']
    settings["kpi_settings"]["fees"] = fee

    ## Entry does not exists?????
    if entry in pnl_data.columns:
        pnl_data = pnl_data.dropna(subset = [entry, exit])

    pnl_data['Direction'] = np.sign(pnl_data[entry + '_Opening_Volume'])
    pnl_data.rename({entry + '_Opening_Volume': 'Volume'}, axis = 1, inplace = True)

    kpi_strategy = Kpi(pnl_data, settings)
    kpi_metrics = kpi_strategy.calculate_kpi_metrics()

    return kpi_metrics


def plot_volume(vol_trades, nomination, mandate):

    fig_vol = px.area(x=vol_trades.index, y = vol_trades)
    if nomination.empty == False:
        fig_vol.add_trace(
        go.Scatter(
            x= nomination['StartTimeUTC'], 
            y= nomination.iloc[:,1],  # Create a list with constant value 2000
            mode='lines', 
            line=dict(color='blue'),
            name='Flow Nomination'
            )
        )
    if nomination.empty == False:
        fig_vol.add_trace(
        go.Scatter(
            x= nomination['StartTimeUTC'], 
            y= nomination.iloc[:,2]*-1,  # Create a list with constant value 2000
            mode='lines', 
            line=dict(color='blue'),
            name='Flow Nomination'
            )
        )
    fig_vol.add_trace(
    go.Scatter(
        x=vol_trades.index, 
        y=[mandate]*len(vol_trades.index),  # Create a list with constant value 2000
        mode='lines', 
        line=dict(color='red'),
        name='Mandate'
        )
    )

    fig_vol.add_trace(
    go.Scatter(
        x=vol_trades.index, 
        y=[-1*mandate]*len(vol_trades.index),  # Create a list with constant value 2000
        mode='lines', 
        line=dict(color='red'),
        name='Mandate'
        )
    )

    return fig_vol

def plot_cum_pnl(pnl_trades, fig):

    pnl_trades.index = pnl_trades.index.tz_convert('Europe/Paris')

    for i in range(1, len(pnl_trades)):
        line_color = "green" if pnl_trades['Profit'].iloc[i] > 0 else "red" if pnl_trades['Profit'].iloc[i] < 0 else "blue"
        fig.add_trace(go.Scatter(
            x=pnl_trades.index[i-1:i+1],
            y=pnl_trades['cum_pnl'].iloc[i-1:i+1],
            mode='lines',
            line=dict(color=line_color),
            showlegend=False
        ), row=1, col=1)


    return fig

def calc_weekday_table(results):

    group = [results.index.dayofweek, results.index.hour]
    summary_hour_week = results.groupby(group).sum()['Profit']
    pnl_dist_calendar = summary_hour_week.unstack(level = 1)/1000
    pnl_dist_calendar = pnl_dist_calendar.round(0)
    day_mapping = {
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wednesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday'}

    pnl_dist_calendar.rename(index=day_mapping, inplace=True)

    return pnl_dist_calendar

def performance_plot(pnl_trades, nomination, kpi_metrics, settings, tab1, signals=None):

    kpi_settings = settings['kpi_settings']
    entry = kpi_settings['entry']
    stats = kpi_metrics['statistics']
    country = kpi_settings['country']
    mandate = kpi_settings[entry][country]['mandate']

    # tab1, tab2, tab3 = st.tabs([ "🗃 Performance", "📈 Trend Following", "📈 Spike and others"])

    hitrate_ = stats.loc[stats['Key Performance Metrics'] == 'Hit Rate', 'All']
    average_profit_ = stats.loc[stats['Key Performance Metrics'] == 'Average Eur/MWh', 'All']

    average_profit, hitrate = tab1.columns(2)

    average_profit.metric(label = "Average Profit Eur/ Mwh", value= average_profit_.values[0])
    hitrate.metric(label="Hitrate %:", value = hitrate_.values[0])

    vol_trades = pnl_trades[entry + '_Opening_Volume']
    pnl_trades = pnl_trades.dropna(subset = ['Profit'])
    pnl_trades['cum_pnl'] = pnl_trades['cum_pnl'].ffill()
    pnl_trades['Profit'] = pnl_trades['Profit'].fillna(0)

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True)

    fig = plot_cum_pnl(pnl_trades, fig)
    fig_vol = plot_volume(vol_trades, nomination, mandate)

    for trace in fig_vol.data:
        fig.add_trace(trace, row=2, col=1)
    
    # Update y-axis labels for each subplot
    fig.update_yaxes(title_text='Cum PnL', row=1, col=1)  # Update this label as needed
    fig.update_yaxes(title_text='Volume MW', row=2, col=1)  # Update this label as needed

    fig.update_layout(width=1920, height=960, legend=dict(
        orientation="h",  # Set legend orientation to horizontal
        xanchor="center",  # Anchor legend to center
        x=0.5,  # Position legend in the center of the x-axis
        y=-0.2  # Position the legend below the chart
       )
    )

    pnl_trades = pnl_trades.sort_index()


    tab1.plotly_chart(fig, use_container_width= True)

    fig_signals = px.line(signals)
    fig_signals.update_layout(legend=dict(orientation="h", xanchor="center", x=0.5,  y=-0.2 ))
    tab1.plotly_chart(fig_signals, use_container_width= True)

    if settings['kpi_settings']['entry'] == 'Total PnL':

        pnl_dist_calendar = calc_weekday_table(pnl_trades)
        fig_table = px.imshow(pnl_dist_calendar, color_continuous_scale=[[0, "red"], [1, "green"]], zmin= -20, zmax= 20 ,text_auto=True)
        fig_table.update_layout(coloraxis_showscale=False)
        fig_table.update_xaxes(nticks=len(pnl_dist_calendar.columns))
        fig_table.update_xaxes(tickmode='linear', dtick=1)
        tab1.plotly_chart(fig_table, use_container_width=True)
    

    # For Weather DAH forecast weekly error
    st.markdown(f"[Met Performance: DAH forecast Weekly error:]({settings['Weather_DAH_Weekly_error']})")
    st.markdown(f"[historical weather dash]({settings['historical_dash']})")
    st.markdown(f"[xbid_flows]({settings['xbid_flows']})")

    # st.plotly_chart(fig_vol, use_container_width= True)
    if settings['kpi_settings']['entry']  in pnl_trades.columns:
        prices = [settings['kpi_settings']['entry'], settings['kpi_settings']['exit']]
        pnl_trades = pnl_trades.dropna(subset = prices)
    tab1.dataframe(pnl_trades,  use_container_width= True)

def intraday_price_plot(intraday_trades, price_levels, country):

    portfolios = intraday_trades['TradingPortfolio'].unique()
    colors = ["green", 'purple', "red", "orange", "brown", "Pink"]  # Extend this list based on your number of portfolios
    color_sequence = [colors[i % len(colors)] for i in range(len(portfolios))]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True)

    scatter = px.scatter(intraday_trades, x='ExecutionTimeUTC', y='Price', color='TradingPortfolio', color_discrete_sequence= color_sequence)

    for trace in scatter.data:
        fig.add_trace(trace, row=1, col=1)
    fig.add_trace( go.Scatter(x=price_levels.index, y=price_levels['imbal'], mode='markers', name='Imbal', marker=dict(color='blue')), row= 1 , col= 1)
    if country not in ["AT", "GB"]:
        fig.add_trace(go.Scatter(x=price_levels.index, y=price_levels['imbal down'], mode='markers', name='Imbal down', marker=dict(color='orange')), row= 1 , col= 1)

    fig.add_hline(y= price_levels['day ahead'][-1], row=1, col=1, line_color = "yellow", line_width=1, name = "day ahead")
    fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', name='Day Ahead', line=dict(color='yellow')))

    if country == "NL":
        fig.add_hline(y= price_levels['MID PRCE'][-1], row=1, col=1, line_color = "pink", line_width=1, name = "mid price")
        fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', name='Mid Price', line=dict(color='pink')))
        fig.add_hline(y= price_levels['POSITIVE PRICE (300)'][-1], row=1, col=1, line_color = "white", line_width=1, name = "mid price")
        fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', name='Ladder + (300)', line=dict(color='white')))

    
    bar = px.bar(intraday_trades, x='ExecutionTimeUTC', y='VolumeMW', color='TradingPortfolio', color_discrete_sequence= color_sequence)
    for trace in bar.data:
        fig.add_trace(trace, row=2, col=1)

    fig.update_layout(height=900, 
        # legend=dict(orientation='h'),
        title_text="Intraday Trades Analysis")
    # Adding axis labels
    # fig.update_xaxes(title_text="X Axis Label", row=1, col=1) # Update as needed for the specific subplot
    fig.update_xaxes(title_text="Date Time CET", row=2, col=1) # Update as needed for the second subplot, if different
    fig.update_yaxes(title_text="Price Eur", row=1, col=1) # Update as needed for the specific subplot
    fig.update_yaxes(title_text="Volume MW", row=2, col=1) # Update as needed for the second subplot, if different

    return fig


def imbal_plot(niv):

    niv.index = niv.index.tz_convert('Europe/Paris')
    igcc_fig = go.Figure()

    for column in niv.columns:
        if column != 'Time':
            igcc_fig.add_trace(go.Scatter(x=niv.index, y=niv[column], fill='tozeroy', name=column))
    
    igcc_fig.update_yaxes(title_text = 'Imbal activation MW')
    
    return igcc_fig

def add_imbal_qty(intraday_trades, price_levels):
    try:
        imbal_qty = price_levels[['ImbalanceQty', 'imbal']]
    except:
        imbal_qty = price_levels['imbal']
    imbal_qty.columns  = ['VolumeMW', 'Price']
    imbal_qty['TradingPortfolio'] = 'Imbal Qty'
    imbal_qty['ExecutionTimeUTC'] = imbal_qty.index
    intraday_trades = pd.concat([intraday_trades, imbal_qty])

    return intraday_trades

def intraday_plot(intraday_trades,  settings, tab2):

    country = settings['kpi_settings']['country']
    trades_meta = intraday_trades._metadata

    if country == "NL":
        price_levels = intraday_trades._metadata[['imbal', 'imbal down', 'day ahead', 'MID PRCE', 'POSITIVE PRICE (100)', 'POSITIVE PRICE (300)', 'ImbalanceQty']]
    elif country == "GB":
        price_levels = intraday_trades._metadata[['imbal', 'day ahead', 'ImbalanceQty']]
    elif country == "FR":
        imbal_col = list(settings['kpi_settings']['imbal']['FR']['imbal_niv'].keys())
        try:
            price_levels = intraday_trades._metadata[imbal_col + ['imbal', 'imbal down', 'day ahead', 'ImbalanceQty']]
        except:
            price_levels = intraday_trades._metadata[imbal_col + ['imbal', 'imbal down', 'day ahead']]
    elif country == "AT":
        price_levels = intraday_trades._metadata[['imbal', 'day ahead']]
        if 'ImbalanceQty' not in price_levels.columns:
            price_levels['ImbalanceQty'] = 0
    else:
        price_levels = intraday_trades._metadata[['imbal', 'imbal down', 'day ahead', 'ImbalanceQty']]

    intraday_trades.index = intraday_trades.index.tz_convert('Europe/Paris')
    price_levels.index = price_levels.index.tz_convert('Europe/Paris')

    intraday_trades = add_imbal_qty(intraday_trades, price_levels)
    fig = intraday_price_plot(intraday_trades, price_levels, country)


    tab2.plotly_chart(fig, use_container_width= True)

    if country == "NL":
        niv = trades_meta[['aFRR UP', 'aFRR DOWN', 'IGCC UP', 'IGCC DOWN']]
        niv[['aFRR DOWN', 'IGCC DOWN']] = -1*niv[['aFRR DOWN', 'IGCC DOWN']]
        imbal_vol_fig = imbal_plot(niv)
        tab2.plotly_chart(imbal_vol_fig, use_container_width = True)
    if country == "FR":
        imbal_vol = price_levels[imbal_col]
        imbal_vol[['afrr_down', 'mfr_down', 'rr_down']] = -1*imbal_vol[['afrr_down', 'mfr_down', 'rr_down']]
        imbal_vol_fig = px.line(imbal_vol)
        tab2.plotly_chart(imbal_vol_fig, use_container_width = True)
    

    
    st.markdown(f"[Met Performance: DAH forecast Weekly error:]({settings['Weather_DAH_Weekly_error']})")
    st.markdown(f"[historical weather dash]({settings['historical_dash']})")
    st.markdown(f"[xbid_flows]({settings['xbid_flows']})")



def kpiv2():

    file = os.path.join(BASE_DIR, "app_config.yaml")

    with open(file, 'r') as stream:
        settings = yaml.safe_load(stream)

    
    st.title('Performance Analysis')
    tab1, tab2, tab3 = st.tabs([ "🗃 Performance",  "📈 Trading and Spikes ", "📈 Trend Following"])

    with tab3:
        # ## CHANGE CONFIG FILE !!!!!
        # with open("config_trend.yaml", 'r') as stream:
        #     settings = yaml.safe_load(stream)

        # st.set_page_config(layout='wide')
        # st.title('Trend and Performance Analysis')
        # end_date = pd.to_datetime('today') - pd.Timedelta(days=1)
        # start_date = end_date - pd.Timedelta(days=8)

        # # defautla fr 0 long 3 
        # country = st.selectbox('Select a country', settings['kpi_settings']['country_list'], index= 3) # change list order to FR
        # weekdays = st.multiselect('Select weekday', options= settings['kpi_settings']['weekdays'], default= settings['kpi_settings']['weekdays'])
        # holidays = st.multiselect('Select holidays', options= settings['kpi_settings']['holidays'], default= 'all_days')
        # entry_price = st.selectbox('Select an entry price', settings['kpi_settings']['price'], index=12)
        # exit_price = st.selectbox('Select an exit price', settings['kpi_settings']['price'], index= 10) #15
        # trend_filter = st.number_input('Trend Filter', value= 1.7)

        # start_date, end_date = st.date_input('Select a date range', value=[start_date, end_date])
        # start_date = pd.to_datetime(start_date)
        # end_date = pd.to_datetime(end_date)  +  pd.Timedelta(days=1)


        # data = get_data(start_date, end_date, country, entry_price, exit_price, settings)
        # data = process_data(data, entry_price, exit_price, weekdays, holidays, country)
        # data = calc_spreads(data, entry_price, exit_price)
        # trends = calc_trends(data, entry_price, exit_price)
        # trend_table = create_table(data, entry_price, exit_price)
        # trend_periods = _sortino_ratio(trend_table, trend_filter)
        # outlier_periods = _z_score(trend_table)
        # create_st_plot(trends, trend_table, trend_periods, outlier_periods, entry_price)
        print('Under Construction')


    with tab2:

        settings['kpi_settings']['country'] = st.selectbox('Select a country', settings['kpi_settings']['country_list'], key = 'unique_country', index= 1)
        show_portfolio = st.checkbox('Show Individual Portfolio')
        cwe_public_trades = st.multiselect('CWE Public trades:', ['DE', 'BE', 'NL', 'FR', 'AT'], default= [])

        country = settings['kpi_settings']['country']
        product = settings['kpi_settings']['intraday_trades'][country]['product']
        product_selection = st.multiselect('select trading products:' , product, default= product)
        default_date = pd.to_datetime('today').floor('d') - pd.Timedelta(days= 2)
        start_date = st.date_input('Select start date for Spike analysis', value = default_date)

        hours = ["HE" + str(i)  for i in range(1, 24)]
        start_time = st.selectbox('Select end hour', hours, index= 13)
        start_time_int = int(start_time.split('HE')[1]) -1
        start_time_str = f"{start_time_int:02d}"
        start_datetime = pd.to_datetime(f'{start_date} {start_time_str}', format='%Y-%m-%d %H')

        intraday_trades = get_intraday_trades(start_datetime, product_selection, show_portfolio, cwe_public_trades, settings)
        intraday_plot(intraday_trades, settings, tab2)

    with tab1:
        end_date = pd.to_datetime('today')
        start_date = end_date - pd.Timedelta(days=8)

        start_date, end_date = st.date_input('Select a date range', value=[start_date, end_date])
        doc_url = settings['docs'] # Replace with your actual documentation URL
        st.markdown(f"[Read the fucking Documentation]({doc_url})")  # Markdown for hyperlink
        settings['kpi_settings']['country'] = st.selectbox('Select a country', settings['kpi_settings']['country_list'], index= 1)
        settings['kpi_settings']['entry'] = st.selectbox('Select trades', settings['kpi_settings']['entry'], index= 1)
        settings['kpi_settings']['exit'] = st.selectbox('Select trades / Benchmark', settings['kpi_settings']['exit'])
        settings['kpi_settings']['weekdays'] = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        settings['kpi_settings']['weekdays_sel'] = st.multiselect('Select weekday', options= settings['kpi_settings']['weekdays'], default= settings['kpi_settings']['weekdays'])

        if settings['kpi_settings']['exit'] == 'ID_vwap_vol':
            vols = list(range(500, 3500, 500))
            settings['kpi_settings']['vol'] = st.selectbox('select Volume: ',vols)


        # add long /short direction
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        country = settings['kpi_settings']['country']
        # country = "AT"
        nomination = get_country_nomination(start_date, end_date, country, settings)
        trades = get_trades_data(start_date, end_date, country, settings)
        signals = get_strategies_for_country(start_date, end_date, country, settings)
        

        pnl_trades = calc_pnl(trades, settings)

        kpi_metrics = calculate_metrics(pnl_trades, settings)
        

        performance_plot(pnl_trades, nomination, kpi_metrics, settings, tab1, signals)



# kpiv2()

