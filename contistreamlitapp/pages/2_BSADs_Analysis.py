import streamlit as st

from pages.BSADs_flows.analysis import BSAS_anaylsis_main

st.set_page_config(page_title="BSAD", layout='wide')

BSAS_anaylsis_main()