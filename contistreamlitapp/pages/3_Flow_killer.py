import streamlit as st
from pages.Flow_killer.flow_killer import flow_killer

st.set_page_config(page_title="Flow Killer", layout='wide')

flow_killer()