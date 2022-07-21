import streamlit as st
from Home import name, authentication_status, username
from streamlit_authenticator import Authenticate, SafeLoader
import yaml

with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)


if 'authentication_status' not in st.session_state:
    st.session_state['authentication_status'] = authentication_status

if 'name' not in st.session_state:
    st.session_state['name'] = name

if 'username' not in st.session_state:
    st.session_state['username'] = username


if st.session_state["authentication_status"]:
    authenticator.logout('Logout', 'main')
    st.write(f'Welcome *{st.session_state["name"]}*')

    with st.sidebar:
        choice = st.selectbox("Select a product", options=["Apple", "Orange", "Grape"], index=0)
        submit = st.button("Change")

    if "product" not in st.session_state:
        st.session_state["product"] = choice

    if submit:
        st.session_state["product"] = choice


    st.write(st.session_state.product)

elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')

