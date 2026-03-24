import os
import yaml
import time
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from pages.Strategy_performance.utilities.data import get_strategy, get_vwap_index, get_vwap, get_ts_db
from pages.Strategy_performance.utilities.kpi_metrics import Kpi
# from contistreamlitapp.pages.Strategy_performance.utilities.data import get_strategy, get_vwap_index, get_vwap, get_ts_db
# from contistreamlitapp.pages.Strategy_performance.utilities.kpi_metrics import Kpi
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor
from functools import partial


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_data(start_date, end_date, strategy, settings):
    start_date = start_date.tz_localize('Europe/Paris').tz_convert('utc')
    end_date = end_date.tz_localize('Europe/Paris').tz_convert('utc')
    fre_min = str(settings[strategy]['freq_min']) + 'min'
    data = pd.DataFrame(index=pd.date_range(start_date, end_date, freq=fre_min))

    entry_price_id = settings[strategy]['id_entry']
    entry_name = settings[strategy]['entry_price']
    exit_price_id = settings[strategy]['id_exit']
    exit_name = settings[strategy]['exit_price']

    # Run get_signal and get_prices_data concurrently

    with ThreadPoolExecutor(max_workers=3) as executor:
        signal_future = executor.submit(get_signal, strategy, start_date, end_date, settings)
        entry_price_future = executor.submit(get_prices_data, start_date, end_date, entry_price_id, entry_name, strategy, settings)
        exit_price_future = executor.submit(get_prices_data, start_date, end_date, exit_price_id, exit_name, strategy, settings)

        signal = signal_future.result()
        entry_price = entry_price_future.result()
        exit_price = exit_price_future.result()
        exit_price = exit_price.resample(fre_min).mean()

    data = data.join([signal, entry_price, exit_price])


    if 'id_exit_back_up' in settings[strategy]:
        exit_backup_id = settings[strategy]['id_exit_back_up']
        exit_backup_name = settings[strategy]['exit_price_backup']
        exit_back_up = get_prices_data(start_date, end_date, exit_backup_id, exit_backup_name, strategy, settings)
        data = data.join(exit_back_up)

    return data

def get_signal(strategy, start_date, end_date, settings):

    if 'dummy' not in strategy:
        id = settings[strategy]['id_strategy']
        signal = get_strategy(start_date, end_date, id)
        signal.loc[signal['Probability'] >3, 'Probability'] = 3 ## TemporaryError to filter prob!! delete wrong records and put error catching in Dagster
        if signal is None or signal.empty:
            st.text_area('No signal generated stopping program')

    else:
        signal = get_dummy_signal(start_date, end_date, strategy)
    if (signal['Signal'].dropna() == 0).all():
        st.text_area('No signal generated stopping program')


    return signal

def get_dummy_signal(start_date, end_date, strategy):
    """
    Short block 3 & 5 excluding weekends 
    """
    data = pd.DataFrame(index = pd.date_range(start_date, end_date, freq= 'h'))
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

    signal['Probability'] = 1
    signal['Volume'] = 50
    signal['CalculationTImeUTC'] =  signal.index.tz_convert('Europe/Paris').floor('D') - pd.Timedelta(hours= 16)
    signal['DeliveryDayCET'] = signal.index.tz_convert('Europe/Paris').floor('D')

    return signal

def get_prices_data(start_date, end_date, price_id, name, strategy, settings):

    double_price_imbalance = ['FR', 'BE', 'NL']
    COUNTRY = name[0:2]

    if any(substring in name for substring in ['Day Ahead', 'day_ahead']):
        price_data = get_ts_db(start_date, end_date, settings, price_id, name)
        price_data = price_data[name]
    
    elif name == "gb_4H_Block_vwap_last_hour":
        product = "4H Block vwap"
        duration = 60
        time_before_delivery = 60

        date_start = start_date - pd.Timedelta(hours= 4)
        date_end = end_date + pd.Timedelta(hours=4)
        price_data = get_vwap('GB', product, date_start, date_end, time_before_delivery, duration)
        price_data = price_data.rename(columns={"VWAP": name})
        price_data = price_data.resample('1h').ffill(limit= 3)
        price_data = price_data.rename(columns={"vwap": name})
        price_data = price_data[[name, 'vwap volume']]

    elif "vwap" in name:

        country = price_id['country']
        product =  price_id['product']
        duration = price_id['duration']
        lead_time = price_id['lead_time']
        price_data = get_vwap_index(country, product, start_date, end_date, lead_time, duration)
        price_data = price_data.rename(columns={'VWAP': name, 'VolumeMWh': name+'vol'})
        price_data = price_data[[name, name+'vol']]

        # If the original data is more granular than the target, we downsample
        if price_id['freq_min'] < settings[strategy]['freq_min']:
            # Downsampling with mean
            target_freq = str(settings[strategy]['freq_min']) + 'min'
            price_data = price_data.resample(target_freq).mean()
        # Otherwise, we upsample
        elif price_id['freq_min'] > settings[strategy]['freq_min']:
            # Upsampling with forward fill
            target_freq = str(settings[strategy]['freq_min']) + 'min'
            limits= (price_id['freq_min']/settings[strategy]['freq_min']) -1
            price_data = price_data.resample(target_freq).ffill(limit= int(limits))
        
        ## ERROR: CALCULATION OF VWAP VOLUME MW VS MWH FOR QUARTER CONVERT FOR THE SHAPING CALCULATION !!!!!!!!
        ## Correct on the back up quarter !!!!
    elif "Imbalance" in name and COUNTRY in double_price_imbalance:
        price_data = pd.DataFrame()
        for id in price_id:
            price_data_i = get_ts_db(start_date, end_date, settings, price_id[id], id)
            price_data = pd.concat([price_data, price_data_i], axis = 1)
        
        price_id_freq = pd.infer_freq(price_data.index.sort_values())
        price_id_freq = float(price_id_freq.split('min')[0])
        if price_id_freq < settings[strategy]['freq_min']:
            # Downsampling with mean
            target_freq = str(settings[strategy]['freq_min']) + 'min'
            price_data = price_data.resample(target_freq).mean()
            # Otherwise, we upsample
        elif price_id_freq > settings[strategy]['freq_min']:
            # Upsampling with forward fill
            target_freq = str(settings[strategy]['freq_min']) + 'min'
            limits= (price_id['freq_min']/settings[strategy]['freq_min']) -1
            price_data = price_data.resample(target_freq).ffill(limit= int(limits))

    elif any(substring in name for substring in ['Imbalance', 'imbalance']):
        price_data = get_ts_db(start_date, end_date, settings, price_id, name)
    
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
    ### Do the upsampling downsampling here!!!!!


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


    # if 'Probability' not in data.columns:
    #    data = data.rename(columns = {'Signal_probability': 'Probability'})

    if strategy == 'nl_flow_export_increase':
        ## for benchmark
        ###
        # data['spread_up'] = data[country + '_imbalance_long'] - data[entry_price]
        # data['spread_down'] = data[country + '_imbalance_short'] - data[entry_price]
        data['spread_up'] = data[country + '_imbalance_short'] - data[entry_price]
        data['spread_down'] = data[country + '_imbalance_long'] - data[entry_price]
    
        col_imbal = ['spread_down', 'spread_up']
        back_up_price = 'nl_vwap_xbid_q_1_5_gc'
        exit_price_name = settings[strategy]['exit_price']
        entry_price_name = settings[strategy]['entry_price']
        vol = settings[strategy]['volume']
        data['original_imbalance'] = data['nl_imbalance_short']

        data['Volume'] = data['Volume'].ffill(limit =3)
        data['Probability'] = data['Probability'].ffill(limit =3)

        data = calculate_exit_price(data, back_up_price, exit_price_name, vol)
        exit_price = 'NL Imbalance vwap shape'

        data[entry_price] = data[entry_price].ffill(limit =3)
        data['Spread'] = data[exit_price] - data[entry_price_name] ## change nl_imbalance_short
        
        freq = 15 #  Quarter
        data['Spread'] = data['Spread']*freq/60
        # data = data.loc[~data.index.hour.isin([16, 17, 18])]
        data.loc[data.index.hour.isin([16, 17, 18]), 'Signal'] = 0


    elif country  in ['nl', 'fr', 'be'] and 'Imbalance' in exit_price:

        data[country + '_imbalance'] = np.nan
        short_mask = data['Signal'] < 0
        long_mask = data['Signal'] > 0
        data.loc[short_mask, country + '_imbalance'] =  data.loc[short_mask, country + '_imbalance_short']
        data.loc[long_mask, country + '_imbalance'] =  data.loc[long_mask, country + '_imbalance_long']
        data[exit_price] = data[country + '_imbalance']
        data['Spread'] = data[exit_price] - data[entry_price]

        data['spread_up'] = data[country + '_imbalance_short'] - data[entry_price]
        data['spread_down'] = data[country + '_imbalance_long'] - data[entry_price]



        col_imbal = ['spread_down', 'spread_up']

    else:
        data['Spread'] = data[exit_price] - data[entry_price]
        col_imbal = []

    
    data['Probability'] = data['Probability'].fillna(1)
    data['Volume'] = data['Probability'].abs()*volume_step
    data['Profit'] = data['Spread']*data['Signal']*data['Volume']
    data['Profit'] = data['Profit'] - data['Volume']*data['Signal'].abs()*fees
    data["cum_pnl"] = data["Profit"].cumsum()



    data['cum_pnl'] = data['cum_pnl'].ffill()
    data = data.rename(columns = {'Signal': 'Direction'})
    col = ['Direction', 'Probability', entry_price, exit_price, 'Spread', 'Volume', 'Profit', 'cum_pnl']
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
    exit_price_name = 'nl_imbalance_short'
    prediction_data = shape_quarter(prediction_data, level, back_up_price, exit_price_name)

    exit_vol = back_up_price+'vol'

    ### round vol to position close volume
    condition = prediction_data[exit_vol] >= prediction_data['Probability']*vol
    prediction_data.loc[condition, exit_vol] = prediction_data['Probability']*vol

    ### Q1 full Volume goes to imbalance therefore intraday volume on Q1 must be zero
    mask_q2q4 = prediction_data.index.minute == 0
    prediction_data.loc[mask_q2q4, exit_vol] = 0

    prediction_data['unfilled_volume'] = prediction_data['Probability']*vol - prediction_data[exit_vol]
    prediction_data['filled_volume'] = prediction_data[exit_vol]

    prediction_data['exit_price_vwap'] = prediction_data.eval(f'(original_imbalance*unfilled_volume + {back_up_price}*filled_volume) / (@vol*Probability)')


    ### Recalculate the spread
    prediction_data = prediction_data.rename({'nl_imbalance_short': 'NL Imbalance vwap shape'}, axis = 1)

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
    prediction_data[back_up_price] = prediction_data[back_up_price].fillna(prediction_data['nl_imbalance_long'])
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
    tz_country ='Europe/Paris' ## define in the future automaticlly
    start_utc = start_date.tz_localize(tz_country).tz_convert('utc')
    end_utc = end_date.tz_localize(tz_country).tz_convert('utc')
    chase_data = pd.DataFrame(index = pd.date_range(start_utc, end_utc, freq= 'h'))

    if country ==  "GB":
        entry_price_benchmark = {'country': "GB", 'product': 'HH vwap', 'duration': 30, 'lead_time': 90, 'freq_min': 30}
    elif country == "FR":
        entry_price_benchmark = {'country': "FR", 'product': "XBID_Hour_Power", 'lead_time': 120, 'duration': 5, 'freq_min': 60}
    elif country == "AT":
        entry_price_benchmark = {'country': "AT", 'product': "XBID_Hour_Power", 'lead_time': 120, 'duration': 5, 'freq_min': 60}
    elif country == "NL":
        entry_price_benchmark = {'country': "NL", 'product': "XBID_Hour_Power", 'lead_time': 120, 'duration': 5, 'freq_min': 60}
    else:
        entry_price_benchmark = settings[strategy]['id_entry']
        entry_price_benchmark['lead_time'] = 90
    entry_price_benchmark['duration'] = 15
    entry_chasing_benchmark = get_prices_data(start_utc, end_utc, entry_price_benchmark, 'vwap 3h before delivery', strategy, settings)

    exit_price_id = settings[strategy]['id_exit']
    exit_name = settings[strategy]['exit_price']
    exit_price = get_prices_data(start_utc, end_utc, exit_price_id, exit_name, strategy, settings)
    exit_price = exit_price.resample('1h').mean()

    chase_data = chase_data.join([entry_chasing_benchmark, exit_price])
    if country in imbal_double_system:
        chase_data = chase_data.join(data[['spread_up', 'spread_down']])
    else:
        chase_data = chase_data.join(data['Spread'])


    
    if country in imbal_double_system:
    
        chase_data['chasing_spread_up'] = chase_data[country.lower() + '_imbalance_short'] - chase_data['vwap 3h before delivery']
        chase_data['chasing_spread_up'] = chase_data['chasing_spread_up'].fillna(chase_data['spread_up']) ##Fill nas with other vwap or spot
        chase_data['chasing_spread_up_lag'] = chase_data['chasing_spread_up'].shift(lag_hours)
        chase_data['chasing_spread_down'] = chase_data[country.lower() + '_imbalance_long'] - chase_data['vwap 3h before delivery']
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
    freq = settings[strategy]['freq_min']

    ## create signal
    data = create_chasing_signal(data, start, end, strategy, settings)
    data['dummy_short_signal'] = -1
    data['dummy_long_signal'] = 1
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

        data['Spread_'] = data['Spread_']*freq/60
        data['Profit' + signal] = data['Spread_']*data[signal]*data['Volume_benchmark']
        data['Profit' + signal] = data['Profit' + signal] - data['Volume_benchmark']*fees
        data["cum_pnl" + signal] = data["Profit" + signal].cumsum()
        
        if freq == '15min':

            data["cum_pnl" + signal] = data["cum_pnl" + signal].ffill(limit = 3)
    


    return data


def create_plot(data, kpis, settings, strategy):

    fig = px.area(data, x= data.index, y = 'cum_pnl', color_discrete_sequence=['green'])

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
    st.text('Note: ROI is based on return on Collateral')
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


def create_stacked_plot(all_data, settings):
    combined_data = pd.DataFrame()
    combined_volume = pd.DataFrame()
    for strategy, data in all_data.items():
        combined_data[strategy] = data['cum_pnl']
        combined_volume[strategy] = data['Volume']*data['Direction']

    chart_type = st.selectbox("Select chart type", ["Area", "Line"])

    if chart_type == "Line":
        fig = px.line(combined_data, x=combined_data.index, y=combined_data.columns,
                      labels={'value': 'Cumulative P&L [EUR]', 'variable': 'Strategy'},
                      title='Cumulative P&L for All Strategies')
    else:  # Area chart
        fig = px.area(combined_data, x=combined_data.index, y=combined_data.columns,
                      labels={'value': 'Cumulative P&L [EUR]', 'variable': 'Strategy'},
                      title='Cumulative P&L for All Strategies')
        fig_vol = px.area(combined_volume, x=combined_volume.index, y=combined_volume.columns,
                          labels={'value': 'Volume [MW]', 'variable': 'Strategy'},
                      title='Volume for All Strategies')

    fig.update_layout(
        xaxis_title="Date Time",
        yaxis_title="Cumulative P & L [EUR]",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5
        ),
        height=800
    )
    fig_vol.update_layout(
        xaxis_title="Date Time",
        yaxis_title="Volume [MW]",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5
        ),
        height=800
    )

    st.plotly_chart(fig, use_container_width=True)
    st.plotly_chart(fig_vol, use_container_width=True)
    

    all_metrics = []
    for strategy, data in all_data.items():
        try:
            kpi_strategy = Kpi(data, strategy, settings)
            kpi_metrics = kpi_strategy.calculate_kpi_metrics()
            metrics = kpi_metrics["statistics"].set_index('Key Performance Metrics')['All']
            metrics.name = strategy
            all_metrics.append(metrics)
        except Exception as e:
            st.warning(f"Error calculating metrics for {strategy}: {str(e)}. Skipping this strategy.")
            continue

    combined_metrics = pd.concat(all_metrics, axis=1).T

    # Reorder columns if needed
    desired_order = [
        'Net PnL', 'VAR', 'ROI', 'Max Daily Draw Down', 'Average GBP/MWh',
        'Hit Rate', 'Net Profit', 'Net Loss', 'Profit Factor', 
        'Number of Winning Trades', 'Number of Losing Trades', 'Total Number of Trades','Max Daily Win'
    ]
    combined_metrics = combined_metrics.reindex(columns=desired_order)

    # Display the combined metrics table
    st.subheader("Strategy Metrics Comparison")
    st.dataframe(combined_metrics.style.format("{:.2f}"))


def process_strategy(strategy, start_date, end_date, settings):
    data = get_data(start_date, end_date, strategy, settings)
    return strategy, proces_data(data, strategy, settings[strategy]['entry_price'], settings[strategy]['exit_price'], settings)

def strategy_perf_parallel(strategies, settings):

    # st.set_page_config(layout='wide')
    st.title('Strategy Performance')
    end_date = pd.to_datetime('today')
    start_date = '2024-01-02'

    start_date, end_date = st.date_input('Select a date range', value=[start_date, end_date])
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date) + pd.Timedelta(days=1)

    start = time.time()
    with ThreadPoolExecutor() as executor:
        process_func = partial(process_strategy, start_date=start_date, end_date=end_date, settings=settings)
        all_data = dict(executor.map(process_func, strategies))
    print(time.time() - start)
    create_stacked_plot(all_data, settings)
    return all_data


file = os.path.join(BASE_DIR, "config_strategy_v3.yaml")

with open(file, 'r') as stream:
    settings = yaml.safe_load(stream)

strategies = settings['kpi_settings']['strategy_list']
# all_data = strategy_perf_parallel(strategies, settings)
