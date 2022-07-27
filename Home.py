import streamlit as st
from PIL import Image
import streamlit_authenticator as stauth
import yaml
from streamlit_authenticator import Authenticate, SafeLoader



# favicon image
im = Image.open("favicon.ico")

st.set_page_config(
    page_title="WhatsApp Blast Optimization",
    page_icon=im,
)

hashed_passwords = stauth.Hasher(['123', '456']).generate()

#st.write(hashed_passwords)

with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)


name, authentication_status, username = authenticator.login('Login', 'main')

if authentication_status:
    with st.sidebar:
        authenticator.logout('Logout', 'main')
    
    st.markdown(
        """
        ### Overview
        The purpose of this job is to optimize the conversion target . 
        By defining this goal, it will help us in gathering and analyzing the data.
        **👈 Select a from the sidebar** to see further analytical insights
        ### Sources of Data
        Until this time, we can do collect several anayltics shown below. 
        We don't limit ourselves to just use below analytics, we might find others in the near future. 
        - __Deals Monitoring__
        Showing daily number of deal or even paid deal based on CRM database updated regularly
        
    """
    )
    
elif authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status == None:
    st.warning('Please enter your username and password')


