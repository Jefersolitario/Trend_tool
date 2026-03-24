import os
import yaml
import numpy as np
import pandas as pd
from pages.Balance_of_Power.data import get_ts_hot_forecast, get_ts_hot_actuals, get_old_mongo_ts_feature, get_async_ts_hot_forecast #, get_energetech_id_flows
from pages.Balance_of_Power.data import get_hydro, get_nuclear_avail, get_nuclear_forecast
import streamlit as st
import plotly.express as px
from sklearn.ensemble import RandomForestClassifier
import shap
import asyncio
import aiohttp


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


@st.cache_data
def get_data_asyn(start, end, config):

    start = pd.to_datetime(start).tz_localize('Europe/Paris').tz_convert('utc')
    end = pd.to_datetime(end).tz_localize('Europe/Paris').tz_convert('utc')
    start = end - pd.Timedelta(hours= 29*24)
    lead_time = config['flows']['lead_time']
    end = end + pd.Timedelta(hours= 24)
    date_range = pd.date_range(start, end, freq= '30min')
    data = pd.DataFrame(index = date_range)
    


    meta_id = config['meta_data_id']
    meta_ids = [meta_id['demand_dah'],  meta_id['demand_id'], meta_id['solar_dah'],
             meta_id[config['solar_selection']], meta_id[config['wind_selection']]]
    names = ['demand_dah', 'demand_id', 'solar_dah', 'solar_id', 'wind_id']

     # Define the list of tasks (each task is a call to get_ts_hot_forecast)
    async def fetch_all_data():
        async with aiohttp.ClientSession() as session:
            tasks = []
            for id, name in zip(meta_ids, names):
                task = get_async_ts_hot_forecast(session, start, end, config, id, name)
                tasks.append(task)
            return await asyncio.gather(*tasks)

    # Run the async function to fetch all data
    fundamental_forecast = asyncio.run(fetch_all_data())
    fund_df = fundamental_forecast[0].join(fundamental_forecast[1:])
    fund_df_30min = fund_df.resample('30min').mean()
    data = data.join(fund_df_30min)


    ### Add outages
    ### Add time of outdates 
    # nuclear_avail = get_nuclear_avail(start, end, config, meta_id['nuclear_avail'])
    nuclear_actuals = get_ts_hot_actuals(start, end, config, meta_id['nuclear_actuals'], 'nuclear_actuals')
    nuclear_gen_forecast = get_nuclear_forecast(start, end, config, meta_id['nuclear_forecast'])
    hydro = get_hydro(start, end, config)    ### CHANGE THIS!!!!
    # flows_id_xbid = get_energetech_id_flows(start, end, 'fr_flows_id', lead_time, config)
    flows_id = get_ts_hot_actuals(start, end,  config, meta_id['flows_id'], 'flows_id')
    ### does it have XBID or Not?
    wind_dah = get_old_mongo_ts_feature(start, end, meta_id['wind_dah'])
    imbal_pos = get_ts_hot_actuals(start, end, config, meta_id['imbal_pos'], 'imbal_pos')
    imbal_neg = get_ts_hot_actuals(start, end, config, meta_id['imbal_neg'], 'imbal_neg')
    imbal_niv = get_ts_hot_actuals(start, end, config, meta_id['imbal_niv'], 'imbal_niv')
    

    list_fundamentals = [nuclear_actuals, nuclear_gen_forecast, wind_dah, flows_id, hydro,
                    imbal_pos, imbal_neg, imbal_niv] #nuclear_avail,

    data = data.join(list_fundamentals)

    return data

def get_nuclear_energetech_forecast(data):

    nuclear_data = data.copy()
    nuclear_data['nuclear energetech_forecast'] = nuclear_data['nuclear_actuals']
    last_non_na_index = nuclear_data['nuclear_actuals'].last_valid_index()
    last_non_na_index_end = last_non_na_index + pd.Timedelta(hours = 3)
    nuclear_data['nuclear energetech_forecast'] = nuclear_data['nuclear energetech_forecast'].fillna(nuclear_data['FR Nuclear gen forecast Day Ahead EQ'])
    mask = (nuclear_data.index > last_non_na_index) & (nuclear_data.index <= last_non_na_index_end)
    nuclear_data.loc[mask, 'nuclear energetech_forecast'] = np.nan

    nuclear_data['nuclear energetech_forecast'] = nuclear_data['nuclear energetech_forecast'].interpolate(method='polynomial', order = 2, axis= 0)
    
    data = data.join(nuclear_data['nuclear energetech_forecast'])

    return data


@st.cache_data
def get_data(start, end, config):

    start = pd.to_datetime(start).tz_localize('Europe/Paris').tz_convert('utc')
    end = pd.to_datetime(end).tz_localize('Europe/Paris').tz_convert('utc')
    start = end - pd.Timedelta(hours= 29*24)


    meta_id = config['meta_data_id']
    lead_time = config['flows']['lead_time']
    end = end + pd.Timedelta(hours= 24)
    date_range = pd.date_range(start, end, freq= '30min')
    data = pd.DataFrame(index = date_range)
    

    ### Add outages
    ### Add time of outdates 
    nuclear_avail = get_nuclear_avail(start, end, config, meta_id['nuclear_avail'])
    hydro = get_hydro(start, end, config)    ### CHANGE THIS!!!!
    # flows_id_xbid = get_energetech_id_flows(start, end, 'fr_flows_id', lead_time, config)
    flows_id = get_ts_hot_actuals(start, end,  config, meta_id['flows_id'], 'flows_id')
    ### does it have XBID or Not?
    demand_dah = get_ts_hot_forecast(start, end, config, meta_id['demand_dah'], 'demand_dah')
    demand_id = get_ts_hot_forecast(start, end, config, meta_id['demand_id'], 'demand_id')
    solar_dah = get_ts_hot_forecast(start, end, config, meta_id['solar_dah'], 'solar_dah')
    solar_id = get_ts_hot_forecast(start, end, config, meta_id[config['solar_selection']], 'solar_id')
    wind_dah = get_old_mongo_ts_feature(start, end, meta_id['wind_dah'])
    wind_id = get_ts_hot_forecast(start, end, config, meta_id[config['wind_selection']], 'wind_id')
    imbal_pos = get_ts_hot_actuals(start, end, config, meta_id['imbal_pos'], 'imbal_pos')
    imbal_neg = get_ts_hot_actuals(start, end, config, meta_id['imbal_neg'], 'imbal_neg')
    imbal_niv = get_ts_hot_actuals(start, end, config, meta_id['imbal_niv'], 'imbal_niv')
    

    list_fundamentals = [demand_dah, demand_id, nuclear_avail, wind_dah, wind_id, solar_dah, solar_id,
                        flows_id, hydro, imbal_pos, imbal_neg, imbal_niv]

    data = data.join(list_fundamentals)
    # data.metadata = nuclear_avail.metadata

    return data


def process_data(data, config):

    """
    using Enappsys intraday flows temporary while we get RNP intraday flows
    """
    # lastest_nuc_name = data.metadata['Nuclear avail latest']
    data.rename({"meteologica_france_wind_powergeneration_forecast_meteologica_total_total_hourly_perc50_-1_11_55": "wind meteo dah"}, axis = 1, inplace = True)
    data['flows_id'] = data['flows_id'].ffill(limit= 1) ## Temporal deactive!
    data['id Flow'] = data['flows_id'] ## Temporal deactive!

    process_data_df = interpolate_data(data, config)

    process_data_df['demand_delta'] = process_data_df['demand_id'] - process_data_df['demand_dah']
    process_data_df['Nuclear gen delta'] = process_data_df['nuclear energetech_forecast'] - process_data_df['FR Nuclear gen forecast Day Ahead EQ']
    process_data_df['solar_delta'] = process_data_df['solar_id'] - process_data_df['solar_dah']
    process_data_df['wind_delta'] = process_data_df['wind_id'] - process_data_df['wind meteo dah']
    process_data_df['hydro_change'] = process_data_df['hydro ror id']  - process_data_df['hydro dah']


    process_data_df['Balance of Power'] = process_data_df.eval("demand_delta - solar_delta - wind_delta - `id Flow` - hydro_change") # Nuclear Avail delta Remove
    process_data_df = process_data_df[2:]


    process_data_df = process_data_df.round(0)


    return process_data_df

def interpolate_data(data, config):

    process_data_df = data.copy()
    col_interpolate = ['solar_dah', 'solar_id' , 'wind_id', 'wind meteo dah'] # include forecast selection
    # process_data_df = data.interpolate(method ='linear')
    process_data_df[col_interpolate] = data[col_interpolate].interpolate(method ='polynomial', order = 3, axis = 0)
    process_data_df.loc[process_data_df['solar_id'] < 0, 'solar_id'] = 0
    process_data_df.loc[process_data_df['solar_dah'] < 0, 'solar_dah'] = 0

    return process_data_df

def find_important_feature(process_data_df, config):
    """"
    2 methods explored here 
    Pearson Correlation : 7 days rolling correlation
    SHAP values: gold standard for identifying features
    source code : https://github.com/Rachnog/Advanced-Deep-Trading/tree/master/feature_importance
    https://medium.com/swlh/ai-in-finance-advanced-idea-research-and-evaluation-beyond-backtests-d4d7bb185854

    """

    ## Change to vwap or price or premium
    data = process_data_df.copy()
    data = data.dropna()
    rolling_corr = data.rolling(window = 48*7).corr(data['imbal_niv'])

    rolling_corr = rolling_corr[48*7:]


    color_cols_test = ['imbal_niv', "demand_delta", "solar_delta", "wind_delta","id Flow", "hydro_change"] #'Balance of Power',
    variable_corr = process_data_df[color_cols_test].corr()['imbal_niv'].abs().sort_values()
    driver_variable = variable_corr.index[-2]

    importance_results = {'drive_var': driver_variable, 'rolling_correlation': rolling_corr}

    return importance_results

def calculate_shaps(data):

    train_data = data.copy()
    train_data = train_data.dropna()
    X_train = train_data.drop(columns=['imbal_niv', 'imbal_pos', 'imbal_neg'])
    y = np.sign(train_data['imbal_niv'])
    w =1./y.shape[0]

    clf = RandomForestClassifier()
    clf.fit(X=X_train,
                y=y,
                sample_weight=w)

    imp = shap_imp(clf, X_train)

    return imp

def shap_imp(clf, X):

    explainer = shap.TreeExplainer(clf)
    shap_values = explainer.shap_values(X)

    fi0 = np.abs(shap_values[0]).mean(axis=0)
    fi1 = np.abs(shap_values[1]).mean(axis=0)
    fi = fi0 + fi1
    imp = pd.DataFrame({
        'feature': X.columns.tolist(),
        'mean': fi
    })
    imp = imp.set_index('feature')

    return imp

def plot_data(process_data_df, importance_results, start, end, config):

    start = pd.to_datetime(start).tz_localize('Europe/Paris').tz_convert('utc')
    end = pd.to_datetime(end).tz_localize('Europe/Paris').tz_convert('utc')
    driver_variable = importance_results['drive_var']
    rolling_corr = importance_results['rolling_correlation']

    mask = (process_data_df.index >= start) & (process_data_df.index <= end + pd.Timedelta(days=1))
    process_data_df = process_data_df.loc[mask]
    process_data_df.index = process_data_df.index.tz_convert('Europe/Paris').tz_localize(None)
    process_data_df.index.name = 'Datetime CET'
    color_cols = ['Balance of Power', driver_variable]
    process_data_df = process_data_df.style.format(precision=1).background_gradient(cmap='seismic_r', vmin= -3000, vmax=3000, axis=None, subset=color_cols)
    st.dataframe(process_data_df, column_order= config['columns_diplay'])

    rolling_corr.index.name = 'Date Time CET'
    col_display = ['id Flow', 'demand_delta', 'solar_delta', 'wind_delta', 'hydro_change', 'Nuclear gen delta', 'Balance of Power']
    rolling_corr_display = rolling_corr[col_display]
    fig = px.line(rolling_corr_display)
    fig.update_layout(
    title='7 Days Rolling Imbalance Driver / Variable Importance',
    xaxis_title='Date Time CET',
    yaxis_title='7 Days Correlation',

    )

    st.plotly_chart(fig, use_container_width= True)

def plot_shap(shap_values):

    shap_values = shap_values.sort_values(by = 'mean')
    shap_fig = px.bar(shap_values, x='mean', y=shap_values.index, orientation='h', title='SHAP Variable Importance')
    st.plotly_chart(shap_fig, use_container_width= True)


def balancepower():

    file = os.path.join(BASE_DIR, "config.yaml")

    with open(file, 'r') as file:
        config = yaml.safe_load(file)


    end_utc = pd.Timestamp.utcnow().tz_convert('Europe/Berlin').floor('d')
    start_utc = end_utc #- pd.Timedelta(hours= 29*24)
    start_date, end_date = st.date_input('Select a date range', value=[start_utc, end_utc])



    config['solar_selection'] = st.selectbox('Select a solar id forecast', ['solar_id_rte', 'solar_id_andrei'])
    config['wind_selection'] = st.selectbox('Select a wind id forecast', ['wind_id_meteo', 'wind_id_andrei'])


    calculate_importance = st.checkbox('Calculate Variable Importance', value=False)

    if st.button('Update'):
        data = get_data_asyn(start_date, end_date, config)
        data = get_nuclear_energetech_forecast(data)
        process_data_df = process_data(data, config)
        importance_results = find_important_feature(process_data_df, config)
        plot_data(process_data_df, importance_results, start_date, end_date, config)

        if calculate_importance:
            shap_importance = calculate_shaps(process_data_df)
            plot_shap(shap_importance)


# balancepower()