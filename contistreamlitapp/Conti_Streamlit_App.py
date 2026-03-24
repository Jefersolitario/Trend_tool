import streamlit as st

st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)

st.write("# Welcome to Conti Power App! 👋")

st.sidebar.success("Select a demo above.")

st.markdown(
    """
    This is a Conti Streamlit App. It is a collection of Apps that are used to monitor and analyse the energy markets.
    
    **👈 Select tool from the sidebar
    ### BSADs Flow Anaylsis?
    - https://energetech.atlassian.net/l/cp/uT23Z07N
    
    ### PnL Performance
    - This is a dash to view the break down of PnL performance per shift
    - Testing out new deployment
    
"""
)

st.caption("Built by Conti Team")
st.caption("v1.0.0")
