from requests import options
import streamlit as st
from Home import check_password


st.markdown("# Page 1")

with st.sidebar:
    choice = st.selectbox("Select a product", options=["Apple", "Orange", "Grape"], index=0)
    submit = st.button("Change")

if "product" not in st.session_state:
    st.session_state["product"] = choice

if submit:
    st.session_state["product"] = choice


st.write(st.session_state.product)