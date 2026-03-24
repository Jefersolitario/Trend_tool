import os
import yaml
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from pages.Strategy_performance.utilities.data import get_spot_prices, get_strategy, get_actuals, get_ts_history, get_vwap_index,  get_vwap
from pages.Strategy_performance.utilities.kpi_metrics import Kpi
# from contistreamlitapp.pages.Strategy_performance.utilities.data import get_spot_prices, get_strategy, get_actuals, get_ts_history, get_vwap_index,  get_vwap
# from contistreamlitapp.pages.Strategy_performance.utilities.kpi_metrics import Kpi
import plotly.graph_objects as go

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_data(start_date, end_date, strategy, settings):

    start_date = start_date.tz_localize('Europe/Paris').tz_convert('utc')
    end_date = end_date.tz_localize('Europe/Paris').tz_convert('utc')
    data = pd.DataFrame(index= pd.date_range(start_date, end_date, freq=settings[strategy]['freq']))

    entry_price_id = settings[strategy]['id_entry']
    entry_name = settings[strategy]['entry_price']
    exit_price_id = settings[strategy]['id_exit']
    exit_name = settings[strategy]['exit_price']


    if 'dummy' not in strategy:
        signal = get_strategy(start_date, end_date, strategy)
    else:
        signal = get_dummy_signal(start_date, end_date, strategy)

    entry_price = get_prices_data(start_date, end_date, entry_price_id, entry_name)
    exit_price = get_prices_data(start_date, end_date, exit_price_id, exit_name)
    exit_price = exit_price.resample(settings[strategy]['freq']).mean()

    data = data.join([signal, entry_price, exit_price])

    if 'id_exit_back_up' in settings[strategy]:
        exit_backup_id = settings[strategy]['id_exit_back_up']
        exit_backup_name = settings[strategy]['exit_price_backup']
        exit_back_up = get_prices_data(start_date, end_date,exit_backup_id ,exit_backup_name)
        # exit_back_up = exit_back_up.resample('1h').mean() # RESOLUTION???
        
        data = data.join(exit_back_up)

    return data

def get_dummy_signal(start_date, end_date, strategy):
    """
    Short block 3 & 5 excluding weekends 
    """
    data = pd.DataFrame(index = pd.date_range(start_date, end_date, freq= 'h'))
    data['hour'] = data.index.tz_convert('Europe/London').hour
    data['weekday'] = data.index.tz_convert('Europe/London').weekday

    ## hours corresponding to block 3 & 5
    block_hours = [7,8,9,10,15,16,17,18]
    weekends = [5,6]
    signal = data.copy()

    signal.loc[signal['hour'].isin(block_hours), 'Signal'] = -1
    signal.loc[signal['weekday'].isin(weekends), 'Signal'] = np.nan

    signal['Signal_probability'] = 1
    signal['Volume'] = 50
    signal['CalculationTImeUTC'] =  signal.index.tz_convert('Europe/Paris').floor('D') - pd.Timedelta(hours= 16)
    signal['DeliveryDayCET'] = signal.index.tz_convert('Europe/Paris').floor('D')

    return signal

def get_prices_data(start_date, end_date, price_id, name):

    double_price_imbalance = ['FR', 'BE', 'NL']
    COUNTRY = name[0:2]

    if "Day Ahead" in name:
        price_data = get_spot_prices(start_date, end_date, price_id, name)
        price_data = price_data[name]
    elif "vwap" in name:

        country = price_id['country']
        product =  price_id['product']
        duration = price_id['duration']
        lead_time = price_id['lead_time']
        price_data = get_vwap_index(country, product, start_date, end_date, lead_time, duration)
        price_data = price_data.rename(columns={'VWAP': name, 'VolumeMWh': name+'vol'})
        price_data = price_data[[name, name+'vol']]
        ## ERROR: CALCULATION OF VWAP VOLUME MW VS MWH FOR QUARTER CONVERT FOR THE SHAPING CALCULATION !!!!!!!!
        ## Correct on the back up quarter !!!!
    elif "Imbalance" in name and COUNTRY in double_price_imbalance:
        price_name_pos = COUNTRY.lower() + "_imbalance_pos"
        price_id_pos = price_id[price_name_pos]
        price_data =  get_actuals(start_date, end_date, price_id_pos, price_name_pos)

        price_name_neg = COUNTRY.lower() + "_imbalance_neg"
        price_id_neg = price_id[price_name_neg]
        price_data_neg =  get_actuals(start_date, end_date, price_id_neg, price_name_neg)
        price_data = pd.concat([price_data, price_data_neg], axis=1, join='inner')

    elif "Imbalance" in name:
        price_data =  get_actuals(start_date, end_date, price_id, name)
    
    elif name == "gb_hh_intraday_last_hour":
        product = "HH vwap"
        duration = 60
        country = name[:2].upper()
        lead_time = None
        date_start = start_date - pd.Timedelta(hours= 2)
        price_data = get_vwap(country, product, date_start,  end_date, lead_time, duration)
        price_data = price_data.sort_values(by = ['DeliveryEndUTC', 'TradeStartUTC'])
        price_data = price_data.drop_duplicates(subset = 'DeliveryEndUTC', keep = 'last')

        price_data = price_data.rename(columns={"VWAP": name})
        price_data = price_data[name]
        price_data = price_data.resample('1h').mean()

    return price_data

def proces_data(data, strategy, entry_price, exit_price, settings):
    """
    Calculate spread from entry and exit price
    handle double price system spread
    calculate lagged imbalanced price for imbalance chasing benchmark
    """

    data = data[~data.index.duplicated(keep='last')]
    volume_step = settings[strategy]['volume']
    fees = settings[strategy]['fee']
    country = exit_price[:2].lower()


    if 'Direction_probability' not in data.columns:
       data = data.rename(columns = {'Signal_probability': 'Direction_probability'})

    if strategy == 'strat_NL_Flow_export_increase':
        ## for benchmark
        data['spread_up'] = data[country + '_imbalance_neg'] - data[entry_price]
        data['spread_down'] = data[country + '_imbalance_pos'] - data[entry_price]
    
        col_imbal = ['spread_down', 'spread_up']
        back_up_price = 'nl_vwap_xbid_q_1_5_gc'
        exit_price_name = settings[strategy]['exit_price']
        entry_price_name = settings[strategy]['entry_price']
        vol = settings[strategy]['volume']
        data['original_imbalance'] = data['nl_imbalance_neg']

        data['Volume'] = data['Volume'].ffill(limit =3)
        data['Direction_probability'] = data['Direction_probability'].ffill(limit =3)

        data = calculate_exit_price(data, back_up_price, exit_price_name, vol)
        exit_price = 'NL Imbalance vwap shape'

        data[entry_price] = data[entry_price].ffill(limit =3)
        data['Spread'] = data[exit_price] - data[entry_price_name] ## change nl_imbalance_neg
        
        freq = 15 #  Quarter
        data['Spread'] = data['Spread']*freq/60
        data = data.loc[~data.index.hour.isin([16, 17, 18])]


    elif country  in ['nl', 'fr', 'be'] and 'Imbalance' in exit_price:

        data[country + '_imbalance'] = np.nan
        short_mask = data['Signal'] < 0
        long_mask = data['Signal'] > 0
        data.loc[short_mask, country + '_imbalance'] =  data.loc[short_mask, country + '_imbalance_neg']
        data.loc[long_mask, country + '_imbalance'] =  data.loc[long_mask, country + '_imbalance_pos']
        data[exit_price] = data[country + '_imbalance']
        data['Spread'] = data[exit_price] - data[entry_price]

        data['spread_up'] = data[country + '_imbalance_neg'] - data[entry_price]
        data['spread_down'] = data[country + '_imbalance_pos'] - data[entry_price]



        col_imbal = ['spread_down', 'spread_up']

    else:
        data['Spread'] = data[exit_price] - data[entry_price]
        col_imbal = []
    
    if strategy == "strat_france_consumption_rte":
        data.loc[data['Signal'] == 1, 'Signal'] = 0
    
    data['Direction_probability'] = data['Direction_probability'].fillna(1)
    data['Volume'] = data['Direction_probability'].abs()*volume_step
    data['Profit'] = data['Spread']*data['Signal']*data['Volume']
    data['Profit'] = data['Profit'] - data['Volume']*data['Signal'].abs()*fees
    data["cum_pnl"] = data["Profit"].cumsum()


    data = data.dropna(subset = ['cum_pnl'])
    data = data.rename(columns = {'Signal': 'Direction'})
    col = ['Direction', 'Direction_probability', entry_price, exit_price, 'Spread', 'Volume', 'Profit', 'cum_pnl']
    fundamentals = settings[strategy]['fundamentals']
    col = col + fundamentals + col_imbal
    data = data[col]

    return data

def calculate_exit_price(prediction_data, back_up_price, exit_price_name, vol):
    """
    if the volume traded is not enough to close Q2-Q4 position the remaining volume will go into imbalance (take imbalance price)
    the exit price needs to be recalculate it, the exit price will be the weighted volume price
    of quarter product and imbalance price

    """
    level = -300
    exit_price_name = 'nl_imbalance_neg'
    prediction_data = shape_quarter(prediction_data, level, back_up_price, exit_price_name)

    exit_vol = back_up_price+'vol'

    ### round vol to position close volume
    condition = prediction_data[exit_vol] >= prediction_data['Direction_probability']*vol
    prediction_data.loc[condition, exit_vol] = prediction_data['Direction_probability']*vol

    ### Q1 full Volume goes to imbalance therefore intraday volume on Q1 must be zero
    mask_q2q4 = prediction_data.index.minute == 0
    prediction_data.loc[mask_q2q4, exit_vol] = 0

    prediction_data['unfilled_volume'] = prediction_data['Direction_probability']*vol - prediction_data[exit_vol]
    prediction_data['filled_volume'] = prediction_data[exit_vol]

    prediction_data['exit_price_vwap'] = prediction_data.eval(f'(original_imbalance*unfilled_volume + {back_up_price}*filled_volume) / (@vol*Direction_probability)')


    ### Recalculate the spread
    prediction_data = prediction_data.rename({'nl_imbalance_neg': 'NL Imbalance vwap shape'}, axis = 1)

    return prediction_data

def shape_quarter(prediction_data, level, back_up_price, exit_price_name):
    """
    Q1 takes imbalance price Q2-Q4 is closed on XBID Quarter intraday
    """
    mask_replace_intraday = prediction_data['flow_change_q4q1'] < level

    # 1 & 2. replace the Q1 with imbalance price and keep vwap for Q2-Q4
    prediction_data.loc[mask_replace_intraday, back_up_price] = prediction_data.loc[mask_replace_intraday, exit_price_name]
    # 3. if no volume was traded on the intraday for Q2-Q4 the position goes into imbalance 
    #### TO DO : CREATE A NEW EXIT NAME IS NOT IMBALANCE PRICE IS EXIT PRICE
    prediction_data[back_up_price] = prediction_data[back_up_price].fillna(prediction_data['nl_imbalance_neg'])
    prediction_data[exit_price_name] = prediction_data[back_up_price]


    # take the signal for the full hour not only quarter
    prediction_data['hour'] = prediction_data.index.hour
    prediction_data['Signal'] = prediction_data['Signal'].ffill(limit = 3)
    # hours_with_signal = prediction_data[prediction_data['Signal'] == 1].index
    # prediction_data.loc[prediction_data.index.floor('h').isin(hours_with_signal), 'Signal'] = 1
    # hours_with_signal = prediction_data[prediction_data['Signal'] == 1]['hour'].unique()
    # prediction_data.loc[prediction_data['hour'].isin(hours_with_signal), 'Signal'] = 1

    return prediction_data

def create_chasing_signal(data, start_date, end_date, strategy, settings):
    """
    Take the imbalance of 3 hour ago as your signal to trade
    entry price: is 3 hour vwap
    exit price:
    """
    lag_hours = 3
    start_date = start_date - pd.Timedelta(hours = lag_hours)
    country = settings[strategy]['country']
    imbal_double_system = ['NL', 'FR', 'BE']
    chase_data = pd.DataFrame(index = pd.date_range(start_date, end_date, freq= 'h', tz ='utc'))

    if country ==  "GB":
        entry_price_benchmark = {'country': "GB", 'product': 'HH vwap', 'duration': 30, 'lead_time': 90}
    else:
        entry_price_benchmark = settings[strategy]['id_entry']
        entry_price_benchmark['lead_time'] = 90
    entry_price_benchmark['duration'] = 15
    entry_chasing_benchmark = get_prices_data(start_date, end_date, entry_price_benchmark, 'vwap 3h before delivery')

    exit_price_id = settings[strategy]['id_exit']
    exit_name = settings[strategy]['exit_price']
    exit_price = get_prices_data(start_date, end_date, exit_price_id, exit_name)
    exit_price = exit_price.resample('1h').mean()

    chase_data = chase_data.join([entry_chasing_benchmark, exit_price])
    if country in imbal_double_system:
        chase_data = chase_data.join(data[['spread_up', 'spread_down']])
    else:
        chase_data = chase_data.join(data['Spread'])


    
    if country in imbal_double_system:
    
        chase_data['chasing_spread_up'] = chase_data[country.lower() + '_imbalance_neg'] - chase_data['vwap 3h before delivery']
        chase_data['chasing_spread_up'] = chase_data['chasing_spread_up'].fillna(chase_data['spread_up']) ##Fill nas with other vwap or spot
        chase_data['chasing_spread_up_lag'] = chase_data['chasing_spread_up'].shift(lag_hours)
        chase_data['chasing_spread_down'] = chase_data[country.lower() + '_imbalance_pos'] - chase_data['vwap 3h before delivery']
        chase_data['chasing_spread_down'] = chase_data['chasing_spread_down'].fillna(chase_data['spread_down']) ##Fill nas with other vwap or spot
        chase_data['chasing_spread_down_lag'] = chase_data['chasing_spread_down'].shift(lag_hours)

        chase_data['chasing_spread_down_lag_abs'] = chase_data['chasing_spread_down_lag'].abs()
        temp_df = chase_data[['chasing_spread_up_lag', 'chasing_spread_down_lag_abs']]
        chase_data['chasing_imbal_signal'] = temp_df.idxmax(axis=1)
        chase_data['chasing_imbal_signal'] = chase_data['chasing_imbal_signal'].map({'chasing_spread_up_lag': 1 ,'chasing_spread_down_lag_abs': -1})

        data = data.join(chase_data[['chasing_imbal_signal', 'chasing_spread_up', 'chasing_spread_down']])
    else: 
        chase_data['spread_chasing'] = chase_data[exit_name] - chase_data['vwap 3h before delivery']
        chase_data['spread_lag2'] = chase_data['spread_chasing'].shift(lag_hours)
        chase_data['chasing_imbal_signal'] = np.sign(chase_data['spread_lag2'])
        data = data.join(chase_data[['chasing_imbal_signal', 'spread_chasing']])
    
    
    return data

def calc_benchmark(data, start, end, strategy, settings):

    volume_step = settings[strategy]['volume']
    fees = settings[strategy]['fee']
    country = settings[strategy]['country']
    imbal_double_system = ['NL', 'FR', 'BE']
    freq = settings[strategy]['freq']

    ## create signal
    data['dummy_short_signal'] = -1
    data['dummy_long_signal'] = 1
    data = create_chasing_signal(data, start, end, strategy, settings)
    ## calc spread
    #profit
    benchamarks = ['dummy_short_signal', 'dummy_long_signal', 'chasing_imbal_signal']

    for signal in benchamarks:

        data['Volume_benchmark'] = volume_step
        
        if country in imbal_double_system:
            if signal == 'chasing_imbal_signal':
                data['Spread_'] = np.where(data[signal] >= 1, data['chasing_spread_down'], data['chasing_spread_up'])
            else:
                data['Spread_'] = np.where(data[signal] >= 1, data['spread_down'], data['spread_up'])
        else:
            data['Spread_'] = data['Spread']
        data['Profit' + signal] = data['Spread_']*data[signal]*data['Volume_benchmark']
        data['Profit' + signal] = data['Profit' + signal] - data['Volume_benchmark']*fees
        data["cum_pnl" + signal] = data["Profit" + signal].cumsum()
        
        if freq == '15min':
            data["cum_pnl" + signal] = data["cum_pnl" + signal].ffill(limit = 3)
    


    return data

def create_plot(data, kpis, settings, strategy):

    fig = px.area(data, x= data.index, y = 'cum_pnl', color_discrete_sequence=['green'])

    # Add a line trace for the same data
    fig.add_trace(
    go.Scatter(x=data.index, y=data['cum_pnldummy_short_signal'], mode='lines', name='Dummy Short Strategy', line=dict(color='red')))
    fig.add_trace(
    go.Scatter(x=data.index, y=data['cum_pnldummy_long_signal'], mode='lines', name='Dummy Long Strategy', line=dict(color='blue')))
    fig.add_trace(
    go.Scatter(x=data.index, y=data['cum_pnlchasing_imbal_signal'], mode='lines', name='Imbalance chasing', line=dict(color='orange')))

    fig.update_layout(
    xaxis_title="Date Time",
    yaxis_title="Cumulative P & L [EUR]",
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=-0.3,  # Position the legend just below the bottom of the plot
        xanchor="center",
        x=0.5   ),
        height=800
    )

    fig_vol = px.area(data, x= data.index, y = 'Volume', color_discrete_sequence=['blue'])
    fig_vol.update_layout(
    xaxis_title="Date Time",
    yaxis_title="Volume [MW]"
    )

    pnl_total = round(kpis.loc[kpis['Key Performance Metrics'] == 'Net PnL', 'All' ], 0)
    average_profit_ = kpis.loc[kpis['Key Performance Metrics'] == 'Average GBP/MWh', 'All' ]
    hitrate_ = kpis.loc[kpis['Key Performance Metrics']== 'Hit Rate', 'All' ]
    
    var_ = kpis.loc[kpis['Key Performance Metrics']== 'VAR', 'All' ]
    var_ = var_.round(0)
    ROI_ = kpis.loc[kpis['Key Performance Metrics']== 'ROI', 'All' ]
    ROI_ = ROI_.round(0)

    cumpnl, average_profit, hitrate, var, ROI = st.columns(5)
    cumpnl.metric(label="P & L : Eur",value = pnl_total)
    average_profit.metric(label = "Average Profit Eur/ Mwh", value= average_profit_)
    hitrate.metric(label="Hitrate %:", value = hitrate_)
    var.metric(label="VAR:", value= var_)
    ROI.metric(label="ROI %:", value= ROI_)
    st.text('Note: ROI is based on return on Collateral, Imbalance chasing takes the imbalance sign 3 hours ago as the direction entry  vwap')
    st.plotly_chart(fig, use_container_width= True)
    st.plotly_chart(fig_vol, use_container_width= True)

    if 'fund_plot' in settings[strategy] and settings[strategy]['fund_plot']:
        y_fundamental = settings[strategy]['fund_plot']
        fig_fundamentals = px.area(data, x= data.index, y= y_fundamental)
        fig_fundamentals.update_layout(
        xaxis_title="Date Time",
        yaxis_title= str(settings[strategy]['fund_plot']),
        showlegend = False
        )
        st.plotly_chart(fig_fundamentals, use_container_width= True)

    st.dataframe(kpis)
    data.index.name = 'Datetime CET'
    data.index = data.index.tz_localize(None)
    data['HE'] = data.index.hour + 1
    data['HE'] = 'HE' +  data['HE'].astype(str)
    st.dataframe(data)
    csv = data.to_csv()#index=False
    b_csv = csv.encode()
    st.download_button(label="Download data as CSV", data=b_csv, file_name="mydata.csv", mime="text/csv")

def strategy_perf():

    file = os.path.join(BASE_DIR, "config_strategy.yaml")

    with open(file, 'r') as stream:
        settings = yaml.safe_load(stream)


    st.title('Strategy Performance')

    end_date = pd.to_datetime('today')
    start_date = end_date - pd.Timedelta(days=46)#8


    strategy = st.selectbox('Select a strategy', settings['kpi_settings']['strategy_list'])
    doc_url = settings[strategy]['docs'] # Replace with your actual documentation URL
    st.markdown(f"[Read the fucking Documentation]({doc_url})")  # Markdown for hyperlink

    entry_price = settings[strategy]['entry_price']
    exit_price = settings[strategy]['exit_price']


    start_date, end_date = st.date_input('Select a date range', value=[start_date, end_date])
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)  +  pd.Timedelta(days=1)
    settings[strategy]['volume'] = st.number_input('select Volume size:', value= settings[strategy]['volume'])

    data = get_data(start_date, end_date, strategy, settings)
    data = proces_data(data, strategy, entry_price, exit_price, settings)
    data = calc_benchmark(data, start_date, end_date, strategy, settings)
    kpi_strategy = Kpi(data, strategy, settings)
    kpi_metrics = kpi_strategy.calculate_kpi_metrics()

    create_plot(data, kpi_metrics["statistics"], settings, strategy)


    return 

# strategy_perf()