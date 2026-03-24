import streamlit as st
from pages.trend_tracker.trends_v4 import trend_tracker_main
st.set_page_config(page_title="Trend Tracker", layout='wide')

trend_tracker_main()