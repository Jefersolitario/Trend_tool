import streamlit as st

from pages.Balance_of_Power.balance_of_power import balancepower

st.set_page_config(page_title="Balance of Power", layout='wide')

balancepower()
