import math
import streamlit as st
from Home import name, authentication_status, username
from streamlit_authenticator import Authenticate, SafeLoader
import yaml
from google.oauth2 import service_account
from google.cloud import storage
import plotly.graph_objects as go
import datetime
import pandas as pd
import numpy as np
import glob
import io
import plotly.express as px
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode, JsCode

st.set_page_config(page_title="Leads Performance", page_icon="📊")
# Create API client google cloud.
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/cloud-platform"])
client = storage.Client(credentials=credentials)


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
    
    ############################## CONTENT START HERE ###########################################
    st.markdown("# Leads Performance")

    # bucket name
    bucket_name ="lead-analytics-bucket"

    ####################### PRODUCT DATAFRAME
    @st.experimental_memo(ttl=3*60*60)
    def get_product_1():
            dtypes = {
                "mt_leads_code": "category",
                "product_name": "category"
            }
            
            data = pd.read_csv("gs://lead-analytics-bucket/crm_db/product_crm.csv",
                storage_options={'token': credentials}, 
                low_memory=False, 
                dtype=dtypes) # read data frame from csv file

            data.drop_duplicates(subset=["mt_leads_code"], keep="first", inplace=True)

            dataframe = data

            return dataframe

    df_product = get_product_1()

    # product dictionary
    product_dict = dict(zip(df_product.mt_leads_code, df_product.product_name))


    ####################### CAMPAIGN DATAFRAME
    url_csv = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQBWaWkI2Omn54cwkule7ykZZQRzH6pdaTVRGFrjPwlAdR51vilty5uCQkQEv27TGNNm7VH_u_cpPe4/pub?output=csv"
    @st.experimental_memo(ttl=3*60*60)
    def get_campaign_1(path):
        df = pd.read_csv(path)
       
        
        return df

    campaign_df = get_campaign_1(url_csv)

    # campaign_tag_dict = {"campaign_id": "campaign_tag"}
    campaign_tag_dict = dict(zip(campaign_df.campaign_id, campaign_df.campaign_tag))

    # main_campaign_dict = {"campaign_id": "main_campaign}
    main_campaign_dict = dict(zip(campaign_df.campaign_id, campaign_df.main_campaign))

    ####################### MAIN DATAFRAME
    @st.experimental_memo(ttl=3*60*60)
    def fetch_db_crm_1():
            dates = ["submit_at", "assign_at", "approved_paid_at", "created_payment","last_update"]
            dtypes = {
                "mt_preleads_code": "category",
                "mt_leads_code": "category",
                "type": "category",
                "campaign_name": "category",
                "assigner": "category",
                "email_sales": "category",
                "m_status_code": "category",
                "outlet_name": "category",
                "owner_phone": "category",
                "rating": "float32",
                "pic_name": "category",
                "full_name": "category",
                "status": "uint8",
                "m_sourceentry_code": "category",
                "counter_followup": "float32",
                "counter_meeting": "float32",
                "channel_name": "category",
                "reject_reason": "category",
                "reject_note": "category"
            }
            
            data = pd.read_csv("gs://lead-analytics-bucket/crm_db/leads_crm.csv",
                storage_options={'token': credentials}, 
                low_memory=False, 
                parse_dates=dates, 
                dtype=dtypes) # read data frame from csv file

            dataframe = data

            # normalize date
            dataframe["submit_at"] = dataframe["submit_at"].dt.normalize()
            dataframe["assign_at"] = dataframe["assign_at"].dt.normalize()
            dataframe["approved_paid_at"] = dataframe["approved_paid_at"].dt.normalize()
            dataframe["created_payment"] = dataframe["created_payment"].dt.normalize()
            dataframe["last_update"] = dataframe["last_update"]

            # cold, hot, warm
            dataframe["leads_potensial_category"] = dataframe.apply(lambda row: 
                                                                            "Cold Leads" if row["rating"] == 1 or row["rating"] == 0
                                                                            else "Warm Leads" if row["rating"] == 2 or row["rating"] == 3
                                                                            else "Hot Leads" if row["rating"] == 4 or row["rating"] == 5
                                                                            else "Null", axis=1)
            # hw by rating
            dataframe["hw_by_rating"] = dataframe.apply(lambda row:
                                                        "hw" if row["leads_potensial_category"] == "Warm Leads"
                                                        else "hw" if row["leads_potensial_category"] == "Hot Leads"
                                                        else "cold" if row["leads_potensial_category"] == "Cold Leads"
                                                        else "Null", axis=1)


            # unnassign, backlog, assigned, junked
            dataframe["status_code"] = dataframe.apply(lambda row:
                                                                "unassigned" if row["status"] == 1
                                                                else "backlog" if row["status"] == 2
                                                                else "assigned" if row["status"] == 3
                                                                else "junked", axis=1)

            # total activity
            dataframe["total_activity"] = dataframe["counter_meeting"] + dataframe["counter_followup"]

            # pipeline
            dataframe["pipeline_by_activity"] = dataframe.apply(lambda row:
                                                            "Pipeline Hot" if "INVOICE" in str(row["m_status_code"])
                                                            else "Pipeline Hot" if row["leads_potensial_category"] == "Hot Leads"
                                                            else "Pipeline Warm" if row["total_activity"] >=2
                                                            else "Pipeline Cold" if row["total_activity"] <=1
                                                            else "Pipeline Null", axis=1)

            # hw by activity
            dataframe["hw_by_activity"] = dataframe.apply(lambda row:
                                                            "hw" if row["pipeline_by_activity"] == "Pipeline Hot"
                                                            else "hw" if row["pipeline_by_activity"] == "Pipeline Warm"
                                                            else "cold" if row["pipeline_by_activity"] == "Pipeline Cold"
                                                            else "Null", axis=1)
                                                                    

            # deal or no deal
            dataframe["deal"] = dataframe.apply(lambda row: 
                                                        "deal" if "PAYMENT" in str(row["m_status_code"])
                                                        else "pipeline" if "INVOICE" in str(row["m_status_code"])
                                                        else "deal" if row["m_status_code"] == "PAID"
                                                        else "leads", axis=1)

            

            # filter only campaign and retouch
            # dataframe = dataframe.loc[dataframe["type"] == "campaign"].copy()

            
            # product_name
            dataframe["product_name"] = dataframe["mt_leads_code"].map(product_dict)

            # campaign_tag
            dataframe["campaign_tag"] = dataframe["campaign_name"].map(campaign_tag_dict)

            # main_campaign
            dataframe["main_campaign"] = dataframe["campaign_name"].map(main_campaign_dict)

            # product deal category (regular or CB)
            dataframe["type_of_product"] = dataframe.apply(lambda row:
                                                                   "CB" if "CB" in str(row["product_name"])
                                                                    else "CB" if "Surprise" in str(row["product_name"])
                                                                    else "Null" if pd.isnull(row["product_name"])
                                                                    else "Regular", axis=1)

            # remove duplicates
            dataframe.drop_duplicates(subset=["mt_preleads_code"], inplace=True)
            
            return dataframe

    ######################## DATA FRAME #########################
    # run function
    data_load_state = st.text('Loading data...')
    df_all = fetch_db_crm_1() 
    data_load_state.text('Loading data...done!')
    st.write(f"Updated at: {df_all.sort_values(by='last_update', ascending=False).iloc[0]['last_update']}")

    ### SIDEBAR ###
    with st.sidebar:

        
        # leads type category
        date_start_data = st.date_input("Select start date", value=datetime.datetime.today().replace(day=1), help="Based on submit at")
        date_end_data = st.date_input("Select end date", value=datetime.datetime.today(), help="Based on submit at")
        type_select = st.selectbox("Type", options=["campaign", "trial"], index=0)
        status_code_select = st.multiselect("Status Code", options=df_all["status_code"].unique(), default="assigned") 

        # SESSION STATE
        if "data_start_date" not in st.session_state:
            st.session_state["data_start_date"] = date_start_data

        if "data_end_date" not in st.session_state:
            st.session_state["data_end_date"] = date_end_data

        if "select_type" not in st.session_state:
            st.session_state["select_type"] = type_select

        if "select_status_code" not in st.session_state:
            st.session_state["select_status_code"] = status_code_select


        # button to update state
        change_date = st.button("Change filter")

        # update the state
        if change_date:
            st.session_state["data_start_date"] = date_start_data
            st.session_state["data_end_date"] = date_end_data
            st.session_state["select_type"] = type_select
            st.session_state["select_status_code"] = status_code_select
    
    

    df_filtered = df_all.loc[
        (df_all["submit_at"].dt.date >= st.session_state["data_start_date"]) &
        (df_all["submit_at"].dt.date <= st.session_state["data_end_date"]) &
        (df_all["type"] == st.session_state["select_type"]) &
        (df_all["status_code"].isin(st.session_state["select_status_code"]))
    ].copy()
    
    

    ############################### AG GRID DATAFRAME ###########################
    # gb = GridOptionsBuilder.from_dataframe(df_filtered)
    
    # update_mode_value = GridUpdateMode.SELECTION_CHANGED
    # return_mode_value = DataReturnMode.FILTERED

    # gb.configure_selection(selection_mode='multiple', use_checkbox=True, groupSelectsFiltered=True, groupSelectsChildren=True)
    # gridOptions = gb.build()

    # grid_response = AgGrid(
    #     df_filtered,
    #     gridOptions=gridOptions,
    #     update_mode=update_mode_value,
    #     return_mode=return_mode_value,
    #     theme='streamlit',
    #     width='100%'
    # )
   


    ############################### HW CAMPAIGNS ###########################
    st.markdown("## Most HW")
    ### date range selected 
    st.markdown(f"Date selected: __{st.session_state['data_start_date']}__ to __{st.session_state['data_end_date']}__")

    tab_hw1, tab_hw2 = st.tabs(["Hw by Activity", "HW by Status"])

    with tab_hw1:
        st.markdown("__Campaigns gaining most HW Activity__")
        st.markdown("_Exclude deal_")
        # dataframe
        df_hw_activity = df_filtered.loc[(df_filtered["hw_by_activity"] == "hw") &
                                        (df_filtered["deal"] != "deal")].copy()
        
        # select layers of campaign items
        col_hw_act1, col_hw_act2, col_hw_act3 = st.columns(3)

        # selection
        hw_act1 = col_hw_act1.selectbox("First Column", options=["m_sourceentry_code", "main_campaign", "campaign_tag", "campaign_name"], index=1, key='act1')
        hw_act2 = col_hw_act2.selectbox("Second Column", options=["m_sourceentry_code", "main_campaign", "campaign_tag", "campaign_name"], index=2, key='act2')
        hw_act3 = col_hw_act3.selectbox("Third Column", options=["m_sourceentry_code", "main_campaign", "campaign_tag", "campaign_name"], index=3, key='act3')

        # SESSION STATE
        if "first_hw_act" not in st.session_state:
            st.session_state["first_hw_act"] = hw_act1

        if "second_hw_act" not in st.session_state:
            st.session_state["second_hw_act"] = hw_act2

        if "third_hw_act" not in st.session_state:
            st.session_state["third_hw_act"] = hw_act3

        # button to update state
        change_act = st.button("Change filter", key="5")

        # update the state
        if change_act:
            st.session_state["first_hw_act"] = hw_act1
            st.session_state["second_hw_act"] = hw_act2
            st.session_state["third_hw_act"] = hw_act3

        # groupby dataframe
        df_hw_act_grouped = df_hw_activity.groupby([st.session_state["first_hw_act"], st.session_state["second_hw_act"], st.session_state["third_hw_act"]])["mt_preleads_code"].count().to_frame().reset_index()
        

        # graph
        fig_hw_act = px.icicle(df_hw_act_grouped, path=[px.Constant("All Data HW by Activity"), st.session_state["first_hw_act"], st.session_state["second_hw_act"], st.session_state["third_hw_act"]], values='mt_preleads_code')
        fig_hw_act.update_traces(root_color="lightgrey", textinfo='percent root+percent parent+label')
        fig_hw_act.update_layout(margin = dict(t=50, l=25, r=25, b=25))
        st.plotly_chart(fig_hw_act, use_container_width=True)
        

    with tab_hw2:
        st.markdown("__Campaigns gaining most HW Rating__")
        st.markdown("_Exclude deal_")
        # dataframe
        df_hw_rating = df_filtered.loc[(df_filtered["hw_by_rating"] == "hw") &
                                        (df_filtered["deal"] != "deal")].copy()
        
        # select layers of campaign items
        col_hw_rating1, col_hw_rating2, col_hw_rating3 = st.columns(3)

        # selection
        hw_rating1 = col_hw_rating1.selectbox("First Column", options=["m_sourceentry_code", "main_campaign", "campaign_tag", "campaign_name"], index=1, key='rating1')
        hw_rating2 = col_hw_rating2.selectbox("Second Column", options=["m_sourceentry_code", "main_campaign", "campaign_tag", "campaign_name"], index=2, key='rating2')
        hw_rating3 = col_hw_rating3.selectbox("Third Column", options=["m_sourceentry_code", "main_campaign", "campaign_tag", "campaign_name"], index=3, key='rating3')

        # SESSION STATE
        if "first_hw_rating" not in st.session_state:
            st.session_state["first_hw_rating"] = hw_rating1

        if "second_hw_rating" not in st.session_state:
            st.session_state["second_hw_rating"] = hw_rating2

        if "third_hw_rating" not in st.session_state:
            st.session_state["third_hw_rating"] = hw_rating3

        # button to update state
        change_rating = st.button("Change filter", key="6")

        # update the state
        if change_rating:
            st.session_state["first_hw_rating"] = hw_rating1
            st.session_state["second_hw_rating"] = hw_rating2
            st.session_state["third_hw_rating"] = hw_rating3

        # groupby dataframe
        df_hw_rating_grouped = df_hw_rating.groupby([st.session_state["first_hw_rating"], st.session_state["second_hw_rating"], st.session_state["third_hw_rating"]])["mt_preleads_code"].count().to_frame().reset_index()
        

        # graph
        fig_hw_rating = px.icicle(df_hw_rating_grouped, path=[px.Constant("All Data HW by Rating"), st.session_state["first_hw_rating"], st.session_state["second_hw_rating"], st.session_state["third_hw_rating"]], values='mt_preleads_code')
        fig_hw_rating.update_traces(root_color="lightgrey", textinfo='percent root+percent parent+label')
        fig_hw_rating.update_layout(margin = dict(t=50, l=25, r=25, b=25))
        st.plotly_chart(fig_hw_rating, use_container_width=True)

    
    ############################## CONTENT END HERE ############################################





elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')