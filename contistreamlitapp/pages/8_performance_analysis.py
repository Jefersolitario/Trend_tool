import streamlit as st

from pages.performance_analysis.performance_analysisv3 import kpiv2

st.set_page_config(page_title="Performance Analysis", layout='wide')

kpiv2()
