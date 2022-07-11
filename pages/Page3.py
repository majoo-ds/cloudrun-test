import streamlit as st
from Home import check_password

if check_password():
    st.markdown("# Page 3")