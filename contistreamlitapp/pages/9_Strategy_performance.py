import streamlit as st

from pages.Strategy_performance.performance_v3_parallel_concurrent import strategy_perf
# from pages.Strategy_performance.performance_v2 import strategy_perf

st.set_page_config(page_title="Strategy Performance", layout='wide')

strategy_perf()
