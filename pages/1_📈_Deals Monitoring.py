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


st.set_page_config(page_title="Deals Monitoring", page_icon="📢")
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
    
    ############################## CONTENT HERE
    
    st.markdown("# Daily Deals Monitoring")

    
    # bucket name
    bucket_name ="lead-analytics-bucket"

    ####################### PRODUCT DATAFRAME
    @st.experimental_memo(ttl=1*60*60)
    def fetch_product():
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

    df_product = fetch_product()

    # product dictionary
    product_dict = dict(zip(df_product.mt_leads_code, df_product.product_name))


    ####################### CAMPAIGN DATAFRAME
    url_csv = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQBWaWkI2Omn54cwkule7ykZZQRzH6pdaTVRGFrjPwlAdR51vilty5uCQkQEv27TGNNm7VH_u_cpPe4/pub?output=csv"
    @st.experimental_memo(ttl=1*60*60)
    def get_campaign_1(path):
        df = pd.read_csv(path)
       
        
        return df

    campaign_df = get_campaign_1(url_csv)

    # campaign_tag_dict = {"campaign_id": "campaign_tag"}
    campaign_tag_dict = dict(zip(campaign_df.campaign_id, campaign_df.campaign_tag))

    # main_campaign_dict = {"campaign_id": "main_campaign}
    main_campaign_dict = dict(zip(campaign_df.campaign_id, campaign_df.main_campaign))



    ###################### ALL DATAFRAME (MAIN) #####################
    # read csv files
    @st.experimental_memo(ttl=1*60*60)
    def fetch_db_crm_monitoring():
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
                "note": "category",
                "extra_note": "category",
                "m_businesstype_code": "category",
                "m_province_name": "category",
                "m_regency_name": "category",
                "outlet_count": "float32",
                "leads_note": "category",
                "journey_name": "category",
                "journey_note": "category",
                "m_reject_reason_code": "category",
                "m_reject_reason_name": "category"
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
                                                            "Pipeline Hot" if row["m_status_code"] == "REQUEST-INVOICE" or row["leads_potensial_category"] == "Hot Leads"
                                                            else "Pipeline Warm" if row["total_activity"] >=2
                                                            else "Pipeline Cold" if row["total_activity"] <=1
                                                            else "Pipeline Null", axis=1)

            # deal or no deal
            dataframe["deal"] = dataframe.apply(lambda row: 
                                                        "deal" if row["m_status_code"] == "REQUESTED-PAYMENT"
                                                        else "pipeline" if row["m_status_code"] == "APPROVED-INVOICE"
                                                        else "deal" if row["m_status_code"] == "PAID"
                                                        else "pipeline" if row["m_status_code"] == "REQUEST-INVOICE"
                                                        else "pipeline" if row["m_status_code"] == "REJECTED-INVOICE"
                                                        else "deal" if row["m_status_code"] == "REJECTED-PAYMENT"
                                                        else "leads", axis=1)

            

            # filter only campaign
            dataframe = dataframe.loc[dataframe["type"] == "campaign"].copy()
            
            # pipeline
            dataframe["pipeline_by_activity"] = dataframe.apply(lambda row:
                                                            "Pipeline Hot" if "INVOICE" in str(row["m_status_code"])
                                                            else "Pipeline Hot" if row["leads_potensial_category"] == "Hot Leads"
                                                            else "Pipeline Warm" if row["total_activity"] >=2
                                                            else "Pipeline Cold" if row["total_activity"] <=1
                                                            else "Pipeline Null", axis=1)

            # deal or no deal
            dataframe["deal"] = dataframe.apply(lambda row: 
                                                        "deal" if "PAYMENT" in str(row["m_status_code"])
                                                        else "pipeline" if row["m_status_code"] == "APPROVED-INVOICE"
                                                        else "deal" if row["m_status_code"] == "PAID"
                                                        else "pipeline" if row["m_status_code"] == "REQUEST-INVOICE"
                                                        else "pipeline" if row["m_status_code"] == "REJECTED-INVOICE"
                                                        else "leads", axis=1)

            

            # filter only campaign and retouch
            dataframe = dataframe.loc[dataframe["type"] == "campaign"].copy()

            
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
            return dataframe

        
                                                    

    # run function
    data_load_state = st.text('Loading data...')
    df = fetch_db_crm_monitoring() 
    data_load_state.text('Loading data...done!')
    st.write(f"Updated at: {df.sort_values(by='last_update', ascending=False).iloc[0]['last_update']}")


    ####### EXPLANATION
    with st.expander("Deal Definition"):
        st.markdown(
            """
            Type of Deals:
        
            __1. Deal:__
            
            Status= REQUESTED-PAYMENT, PAID, REJECTED-PAYMENT
            
            __1. Pipeline:__
            
            Status = APPROVED-INVOICE, REQUEST-INVOICE, REJECTED-INVOICE
            
            __2. Leads:__
            
            Status = Beyond, e.g: NEW, FOLLOW-UP
            
        
        """
        )
    

    ####################### ALL ASSIGNED and RETOUCHED DATAFRAME ########################
    # section title
    st.subheader("Assigned and Retouched Leads")
    st.markdown("Based on __Submit Date__")

    # selection widget
    col_assigned1, col_assigned2, col_assigned3 = st.columns(3)
    # leads type category
    date_start_assigned = col_assigned1.date_input("Select start date", value=datetime.datetime.today().replace(day=1), help="Based on submit at")
    date_end_assigned = col_assigned2.date_input("Select end date", value=datetime.datetime.today(), help="Based on submit at")
    num_of_min_activity = col_assigned3.selectbox("Min of Activities", options=range(0,11), index=2, help="Select minimum of total activities to be categorized as 'retouched'. Default is 2")
    

    # SESSION STATE
    if "assigned_end_date" not in st.session_state:
        st.session_state["assigned_start_date"] = date_start_assigned

    if "assigned_end_date" not in st.session_state:
        st.session_state["assigned_end_date"] = date_end_assigned

    if "assigned_act_num" not in st.session_state:
        st.session_state["assigned_act_num"] = num_of_min_activity

    # button to update state
    change_data = st.button("Change filter", key="0")

    # update the state
    if change_data:
        st.session_state["assigned_start_date"] = date_start_assigned
        st.session_state["assigned_end_date"] = date_end_assigned
        st.session_state["assigned_act_num"] = num_of_min_activity
    
    # total assigned
    df_assigned = df.loc[
        (df["status_code"] == "assigned") &
        (df["submit_at"].dt.date >= st.session_state["assigned_start_date"]) &
        (df["submit_at"].dt.date <= st.session_state["assigned_end_date"])
    ].copy()

    df_retouched = df_assigned.loc[
        (df_assigned["m_status_code"].isin(["APPROVED-INVOICE", "FOLLOW-UP", "PAID", "REJECTED-INVOICE", "REJECTED-PAYMENT", "REQUEST-INVOICE", "REQUESTED-PAYMENT"])) &
        (df_assigned["total_activity"] >= st.session_state["assigned_act_num"])
    ].copy()

    df_new_rejected = df_assigned.loc[
        df_assigned["m_status_code"].isin(["NEW", "REJECTED-LEADS"])
    ].copy()

    ############################ FISRT SUMMMARY METRIC = TOTAL ASSIGN and TOTAL RETOUCH
    # create columns 
    col_met1, col_met2, col_met3, col_met4 = st.columns(4)

    len_assigned = len(df_assigned)
    len_retouched = len(df_retouched)
    len_new_rejected = len(df_new_rejected)

    col_met1.metric("Total assigned", value=format(len_assigned, ","), delta=f"{len_assigned/len_assigned:.2%}", help="All leads assigned to telesales")
    col_met2.metric("Total retouched", value=format(len_retouched, ","), delta="{:.2%}".format(len_retouched/len_assigned), help=f"Num of leads followed up more than { st.session_state.assigned_act_num} activities")
    col_met3.metric("Total new and rejected", value=format(len_new_rejected, ","), delta=f"{len_new_rejected/len_assigned:.2%}", help="Num of new leads and rejected leads")
    col_met4.metric("Total gap", value=format(len_retouched + len_new_rejected - len_assigned, ","), delta=f"{(len_retouched + len_new_rejected - len_assigned)/len_assigned:.2%}", help="Num of leads left as follow up")
    
    # average number of activities per lead
    st.markdown(f"Average number of activities per lead: __{df_assigned['total_activity'].mean():.2f}__")
    
     ####################### ASSIGNED and RETOUCHED DATAFRAME ########################
    # section title
    st.subheader("Assigned and Retouched Based on Campaign")
    st.markdown("Based on __Submit Date__")
    
    tab_campaign1, tab_campaign2 = st.tabs(["Main Campaign", "Campaign Tag"])

    with tab_campaign1:
        st.subheader("Campaign Tag")
        # assigned dataframe
        df_assigned_tag = df_assigned.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_assigned_tag.columns = ["campaign_tag", "assigned_leads"]
        
        # retouched dataframe
        df_retouched_tag = df_retouched.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_retouched_tag.columns = ["campaign_tag", "retouched_leads"]

        # new and rejected dataframe
        df_new_rejected_tag = df_new_rejected.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_new_rejected_tag.columns = ["campaign_tag", "new_rejected_leads"]

        df_campaign_tag = pd.merge(df_assigned_tag, df_retouched_tag, how="left", left_on="campaign_tag", right_on="campaign_tag").merge(df_new_rejected_tag, how="left", right_on="campaign_tag", left_on="campaign_tag").fillna(0)
        df_campaign_tag["retouched_leads"] = df_campaign_tag["retouched_leads"].astype("int")
        df_campaign_tag["new_rejected_leads"] = df_campaign_tag["new_rejected_leads"].astype("int")
        df_campaign_tag["gap_leads"] = df_campaign_tag["assigned_leads"] - df_campaign_tag["retouched_leads"] - df_campaign_tag["new_rejected_leads"]

        grid_df_campaign_tag = AgGrid(df_campaign_tag, editable=True, key="1")
        new_df_campaign_tag = grid_df_campaign_tag["data"]

        
    
    with tab_campaign2:
        st.subheader("Main Campaign")

    ############################## END OF CONTENT
    


elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')

