import logging
import math
import time
from datetime import datetime, timedelta
import streamlit as st
import pandas as pd
import pytz

import plotly.express as px
import plotly.graph_objects as go

from utils.mongo import client
# from contistreamlitapp.utils.mongo import client

from streamlit_autorefresh import st_autorefresh


logger = logging.getLogger(f"contistreamlit.{__name__}")

BERLIN_TIMEZONE = pytz.timezone("Europe/Berlin")
LONDON_TIMEZONE = pytz.timezone("Europe/London")
DUBAI_TIMEZONE = pytz.timezone("Asia/Dubai")
UTC_TIMEZONE = pytz.timezone("UTC")

def compute_open_close(data: pd.DataFrame) -> pd.DataFrame:
    data["AbsBuyQty"] = data["BuyQty"].abs()
    data["ClosePosition"] = data[["SellQty", "AbsBuyQty"]].min(axis=1)
    data["OpenPosition"] = data["BuyQty"].abs() - data["SellQty"].abs()
    open_df = data.drop(columns=["AbsBuyQty"])
    return open_df


def compute_pnl_per_bucket(trades: pd.DataFrame) -> pd.DataFrame:
    trades = trades.reset_index(drop=True)
    trades["PnLRealized"] = 0

    trades["PnLRealized"] = (trades["ClosePosition"] * trades["SellVWAPRealized"]) - (
        trades["ClosePosition"] * trades["BuyVWAPRealized"]
    )
    trades["AbsQty"] = trades["SellQty"] + trades["BuyQty"]
    trades["PnLRealized"] = trades["PnLRealized"] - 2 * trades["ClosePosition"] * (
        trades["FeeValue"] / trades["AbsQty"]
    )

    return trades.drop(columns=["AbsQty"])


def get_fx_gbp_euro(date_from: datetime, date_to: datetime) -> pd.DataFrame:
    df_fx = pd.DataFrame(
        list(
            client["Forex"]["ClosingFx"].find(
                {
                    "currency_from": "EUR",
                    "currency_to": "GBP",
                    "date": {
                        "$gte": date_from - timedelta(days=10),
                        "$lt": date_to + timedelta(days=10),
                    },
                },
                {"_id": False, "date": True, "rate": True},
            )
        )
    )
    df_fx["rate"] = df_fx["rate"].astype(float)
    df_fx["rate"] = 1 / df_fx["rate"]

    df_ref = pd.DataFrame(
        pd.date_range(
            start=date_from - timedelta(days=10), end=date_to + timedelta(days=10)
        ),
        columns=["date"],
    )
    df_ref["date"] = df_ref["date"].dt.normalize()
    df_fx["date"] = df_fx["date"].dt.tz_convert("Europe/Berlin")
    df_fx = pd.merge(df_ref, df_fx, how="left")
    df_fx = df_fx.sort_values("date")
    df_fx["rate"] = df_fx["rate"].ffill()
    df_fx = df_fx.sort_values("date", ascending=[False])
    df_fx["rate"] = df_fx["rate"].ffill()
    df_fx = df_fx.sort_values("date")
    df_fx["Currency"] = "GBP"
    df_fx['date'] = pd.to_datetime(df_fx['date'])
    return df_fx


def get_capacity_data(start_time_utc):

    find_clause = {
        "StartTimeUTC": {
            "$gte": start_time_utc ,
        },
    }

    cap_trades = pd.DataFrame(
        list(
            client['TradeData']['PowerCapacity'].find(
                find_clause,
                {
                    '_id': 0,
                    'ExecutionTimeUTC': 1,
                    'StartTimeUTC': 1,
                    'EndTimeUTC': 1,
                    'TradingPortfolio': 1,
                    'VolumeMW': 1,
                    'Price': 1,
                    'CommodityFamily': 1,
                }
            )
        )
    )

    cap_trades['PnLRealized'] = -1 *(cap_trades['VolumeMW'] * cap_trades['Price'])

    cap_trades['Country'] = 'CAP'

    cap_trades['TradingPortfolioGroup'] = cap_trades['TradingPortfolio'] + 'CAP'

    return cap_trades.groupby(['StartTimeUTC', "TradingPortfolioGroup", 'Country'])['PnLRealized'].sum().reset_index()


@st.cache_data(ttl=10*60)
def get_data():


    start_time_utc = BERLIN_TIMEZONE.localize(
        datetime.now().replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
    ).astimezone(UTC_TIMEZONE)

    df_positions = pd.DataFrame(
        client["TradeData"]["Positions"].find(
            {
                "StartTimeUTC": {"$gte": start_time_utc},
                "TradingPortfolioGroup": {"$regex": "Continent", "$options": "i"},
            },
            {
                "_id": 0,
                "TradingPortfolioGroup": 1,
                "Country": 1,
                "StartTimeUTC": 1,
                "EndTimeUTC": 1,
                "BuyVWAPRealized": 1,
                "SellVWAPRealized": 1,
                "BuyQty": 1,
                "SellQty": 1,
                "FeeValue": 1,
                "ExecutionDayCET":1,
                "Currency":1
            },
        )
    )

    df_positions = compute_open_close(df_positions)

    df_positions = compute_pnl_per_bucket(df_positions)

    df_fx = get_fx_gbp_euro(
        df_positions["ExecutionDayCET"].min().to_pydatetime(),
        df_positions["ExecutionDayCET"].max().to_pydatetime(),
    )

    df_positions = pd.merge(
        df_positions,
        df_fx,
        right_on=["date", "Currency"],
        left_on=["ExecutionDayCET", "Currency"],
        how="left",
    )
    df_positions["rate"] = df_positions["rate"].fillna(1)
    df_positions = df_positions.drop(columns=["date"])


    df_positions['PnLRealized'] = df_positions['PnLRealized'] * df_positions["rate"]
    df_positions = df_positions.drop(columns=["rate"])

    cap_data = get_capacity_data(start_time_utc)
    df_positions = pd.concat([df_positions, cap_data])

    df_positions["StartTimeCET"] = df_positions["StartTimeUTC"].dt.tz_convert(
        BERLIN_TIMEZONE
    )
    df_positions["Shift"] = ''
    df_positions["Shift"] = df_positions["StartTimeCET"].dt.hour.apply(
        lambda x: 'Night' if 0 <= x < 6 else ('Morning' if 6 <= x < 14 else 'Evening')
    )

    df_daily_pnl = df_positions.groupby(
        ["TradingPortfolioGroup", "Country", "StartTimeCET", "Shift"]
    )["PnLRealized"].sum().reset_index()


    df_daily_pnl['Date'] = df_daily_pnl['StartTimeCET'].dt.date
    df_daily_pnl['Month'] = df_daily_pnl['StartTimeCET'].dt.month
    df_daily_pnl['Week'] = df_daily_pnl['StartTimeCET'].dt.isocalendar().week
    df_daily_pnl['Year'] = df_daily_pnl['StartTimeCET'].dt.year

    return df_daily_pnl


def create_plot(data, stack_period = 'D'):
    data['Date'] = pd.to_datetime(data['Date'])

    data_cap = data[data['Country'] == 'CAP'].copy()

    daily_cumulative_pnl = data.groupby('Date')['PnLRealized'].sum().cumsum().reset_index()
    daily_cumulative_cap_cost = data_cap.groupby('Date')['PnLRealized'].sum().cumsum().reset_index()


    if stack_period == 'D':
        stack_bar_data = data.groupby(['Date', 'Shift'])['PnLRealized'].sum().reset_index()
    if stack_period == 'M':
        stack_bar_data = data.groupby([data['Date'].dt.to_period('M'), 'Shift'])['PnLRealized'].sum().reset_index()
        stack_bar_data['Date'] = pd.to_datetime(stack_bar_data['Date'].astype(str))
    if stack_period == 'W':
        stack_bar_data = data.groupby([data['Date'].dt.to_period('W-MON'), 'Shift'])['PnLRealized'].sum().reset_index()
        stack_bar_data['Date'] = pd.to_datetime(stack_bar_data['Date'].astype(str).str.split('/').str.get(0))


    pivot_df = stack_bar_data.pivot_table(values='PnLRealized', index='Date', columns='Shift', aggfunc='sum')
    pivot_df = pivot_df.reset_index()

    # fig = px.bar(pivot_df, x='Date', y=['Morning', 'Evening', 'Night'], title='Cumlative Conti PnL', labels={'value': 'PnL Realized'}, height=600)
    #
    # fig.add_scatter(x=daily_cumulative_pnl['Date'], y=daily_cumulative_pnl['PnLRealized'], mode='lines', name='Total PnL')
    #
    # # Display the plot in Streamlit
    # st.plotly_chart(fig, use_container_width=True,)

    fig = go.Figure()

    # Add bar traces (as in your initial setup)
    if 'Morning' in pivot_df.columns:
        fig.add_trace(go.Bar(x=pivot_df['Date'], y=pivot_df['Morning'], name='Morning', base="relative"))
    if 'Evening' in pivot_df.columns:
        fig.add_trace(go.Bar(x=pivot_df['Date'], y=pivot_df['Evening'], name='Evening', base="relative"))
    if 'Night' in pivot_df.columns:
        fig.add_trace(go.Bar(x=pivot_df['Date'], y=pivot_df['Night'], name='Night', base="relative"))

    # Add scatter trace with its own y-axis
    fig.add_trace(
        go.Scatter(x=daily_cumulative_pnl['Date'], y=daily_cumulative_pnl['PnLRealized'],
                   mode='lines', name='Total PnL', yaxis='y2', line=dict(color='green'))
    )

    fig.add_trace(
        go.Scatter(x=daily_cumulative_cap_cost['Date'], y=daily_cumulative_cap_cost['PnLRealized'],
                   mode='lines', name='Cap Costs', yaxis='y2', line=dict(color='red'))
    )

    # Update layout to include the secondary y-axis
    fig.update_layout(
        yaxis2=dict(
            title='PnL Realized',
            overlaying='y',
            side='right'
        ),
        barmode='stack',
        title='Cumlative Conti PnL',
        height=600
    )

    st.plotly_chart(fig, use_container_width=True,)




def create_pivot_table(data, country = None):

    if country:
        label_str = f'{country} Total'
    else:
        label_str = f'Total'
    df_pivot = data.pivot(index='Shift', columns='Date', values='PnLRealized')
    df_pivot.loc[label_str] = df_pivot.sum(numeric_only=True)
    df_pivot = df_pivot.reindex([label_str, 'Morning', 'Evening', 'Night'])

    # if country:
    #     df_pivot = df_pivot.rename(index={'Morning': f'{country} Morning', 'Evening': f'{country} Evening', "Night": f"{country} Night"})

    return df_pivot[sorted(list(df_pivot.columns), reverse=True)]


def create_table(pnl_per_period):

    countries = list(pnl_per_period['Country'].unique())
    if 'CAP' in countries:
        countries.remove('CAP')
        countries.append('CAP')
    total_data = pnl_per_period.groupby(['Date', "Shift"])["PnLRealized"].sum().reset_index()
    totals_table = create_pivot_table(total_data)
    for country in countries:
        country_data = pnl_per_period[pnl_per_period['Country'] == country].copy()
        country_table = create_pivot_table(country_data, country=country)
        totals_table = pd.concat([totals_table, country_table])

    totals_table.reset_index(inplace=True)

    totals_table = totals_table.applymap(format_cells)

    table_styled = totals_table.style.apply(style_row, axis=1)

    table_styled.set_properties(**{
        'border-bottom': 'double',
        'border-bottom-color': 'white'
    }, subset=pd.IndexSlice[totals_table.index[3::4], :])

    html = table_styled.hide(axis="index").to_html()

    scrollable_html = f"""
    <div style='height: 1000px; font-size: 16px; overflow-y: auto; border: 1px solid #ccc; padding: 10px;'>
        {html}
    """

    # Display the HTML in Streamlit
    st.write(scrollable_html, unsafe_allow_html=True)

    # st.dataframe(table_styled)


def format_cells(val):

    if isinstance(val, (int, float)):
        if math.isnan(val):
            return ''
        if (val > 1000) or (val < -1000):
            return f'{val / 1000:.2f}k'
        else:
            return f'{val:.2f}'
    return val


def apply_background_color(val, idx):
    style = ''
    try:
        val = float(val.replace('k', '').replace(',', ''))  # Convert formatted string back to float
    except ValueError:
        val = 0.0
    if val == 0:
        style = style
    elif val > 0:
        style += 'color: green; '
    else:
        style += 'color: red; '

    if (idx == 0) or (idx % 4 ==0):
        style += 'font-weight: bold; font-size: 16px;'
    else:
        style += 'font-size: 14px;'
    return style

def style_row(row):
    idx = row.name
    return row.apply(lambda val: apply_background_color(val, idx))

def pnl_dash():
    count = st_autorefresh(interval=30000, key="pnl_perf_counter")

    pnl_per_period = get_data()

    time_now_cet = BERLIN_TIMEZONE.localize(
        datetime.now()
    )

    daily = pnl_per_period.groupby(['Date', "Country", 'Shift'])["PnLRealized"].sum().reset_index()
    week = pnl_per_period.groupby(['Week', "Country", 'Shift'])["PnLRealized"].sum().reset_index().rename(columns={'Week': "Date"})
    month = pnl_per_period.groupby(['Month', "Country", 'Shift'])["PnLRealized"].sum().reset_index().rename(columns={'Month': "Date"})
    year = pnl_per_period.groupby(['Year', "Country", 'Shift'])["PnLRealized"].sum().reset_index().rename(columns={'Year': "Date"})


    today = daily[daily['Date'] == time_now_cet.date()].copy()
    today['Date'] = 'Daily'

    yesterday = daily[daily['Date'] == time_now_cet.date()- timedelta(1)].copy()
    yesterday['Date'] = 'D-1'

    weekly = week[week['Date'] == time_now_cet.isocalendar().week].copy()
    weekly['Date'] = 'Weekly'

    monthly = month[month['Date'] == time_now_cet.month].copy()
    monthly['Date'] = 'Monthly'

    yearly = year[year['Date'] == time_now_cet.year].copy()
    yearly['Date'] = 'Yearly'

    summary_data = pd.concat(
        [
            today,
            yesterday,
            weekly,
            monthly,
            yearly
        ]
    )

    summary_tab, daily_tab, weekly_tab, monthly_tab = st.tabs(["Summary", "Daily", "Weekly", "Monthly"])


    with summary_tab:
        data_container = st.container()
        with data_container:
            plot, = st.columns(1)
            table, = st.columns(1)
            with table:
                create_table(summary_data)
            with plot:
                create_plot(pnl_per_period)

                # # Create columns to arrange the charts side by side
                # col1, col2 = st.columns(2)
                #
                # # Place the charts in the respective columns
                # with col1:
                #     fig = px.pie(daily, values='PnLRealized', names='Country', title='Country split of Positive PnL')
                #     st.plotly_chart(fig, use_container_width=True, height=1000)
                #
                # with col2:
                #     fig = px.pie(daily, values='PnLRealized', names='Shift', title='Shift Split of Positive PnL')
                #     st.plotly_chart(fig, use_container_width=True, height=1000)


    with daily_tab:
        data_container = st.container()
        with data_container:
            plot, = st.columns(1)
            table, = st.columns(1)
            with table:
                create_table(daily)
            with plot:
                create_plot(pnl_per_period.copy())

    with weekly_tab:
        data_container = st.container()
        with data_container:
            plot, = st.columns(1)
            table, = st.columns(1)
            with table:
                create_table(week)
            with plot:
                create_plot(pnl_per_period.copy(), stack_period='W')
    with monthly_tab:
        data_container = st.container()
        with data_container:
            plot, = st.columns(1)
            table, = st.columns(1)
            with table:
                create_table(month)
            with plot:
                create_plot(pnl_per_period.copy(), stack_period='M')



if __name__ == "__main__":

    pnl_per_period = get_data()

    time_now_cet = BERLIN_TIMEZONE.localize(
        datetime.now()
    )

    daily = pnl_per_period.groupby(['Date', "Country", 'Shift'])["PnLRealized"].sum().reset_index()
    week = pnl_per_period.groupby(['Week', "Country", 'Shift'])["PnLRealized"].sum().reset_index().rename(columns={'Week': "Date"})
    month = pnl_per_period.groupby(['Month', "Country", 'Shift'])["PnLRealized"].sum().reset_index().rename(columns={'Month': "Date"})
    year = pnl_per_period.groupby(['Year', "Country", 'Shift'])["PnLRealized"].sum().reset_index().rename(columns={'Year': "Date"})

    today = daily[daily['Date'] == time_now_cet.date()].copy()
    today['Date'] = 'Daily'

    yesterday = daily[daily['Date'] == time_now_cet.date()].copy()
    yesterday['Date'] = 'D-1'

    weekly = week[week['Date'] == time_now_cet.isocalendar().week].copy()
    weekly['Date'] = 'Weekly'

    monthly = month[month['Date'] == time_now_cet.month].copy()
    monthly['Date'] = 'Monthly'

    yearly = year[year['Date'] == time_now_cet.year].copy()
    yearly['Date'] = 'Yearly'

    summary_data = pd.concat(
        [
            today,
            yesterday,
            weekly,
            monthly,
            yearly
        ]
    )

    create_plot(pnl_per_period, stack_period='M')





    print(daily)
