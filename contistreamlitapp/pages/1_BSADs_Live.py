import streamlit as st

from pages.BSADs_flows.live_view import bsads_flow_dash

st.set_page_config(page_title="BSAD", layout='wide')

bsads_flow_dash()