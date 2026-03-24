import streamlit as st

from pages.DAH_flow_tracker.dah_flows import dah_flows_main

st.set_page_config(page_title="Strategy Performance", layout='wide')

dah_flows_main()
