import streamlit as st

from pages.PnL_Perf_Dash.pnl import pnl_dash

st.set_page_config(page_title="PnL Performance", layout='wide')


pnl_dash()