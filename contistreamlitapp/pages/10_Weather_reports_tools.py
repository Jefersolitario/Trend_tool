import streamlit as st

from pages.weather_performance.report_tools import weather_tools_links

st.set_page_config(page_title="Weather tools", layout='wide')

weather_tools_links()
