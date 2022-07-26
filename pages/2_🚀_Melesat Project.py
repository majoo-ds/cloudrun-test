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

st.set_page_config(page_title="Telesales Melesat", page_icon="📊")
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
    st.markdown("# Melesat Project Monitoring")

    # bucket name
    bucket_name ="lead-analytics-bucket"

    ####################### PRODUCT DATAFRAME
    @st.experimental_memo(ttl=1*60*60)
    def get_product():
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

    df_product = get_product()

    # product dictionary
    product_dict = dict(zip(df_product.mt_leads_code, df_product.product_name))


    @st.experimental_memo(ttl=1*60*60)
    def fetch_db_crm_melesat():
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

            

            # filter only campaign and retouch
            dataframe = dataframe.loc[(dataframe["type"] == "campaign") & (dataframe["m_sourceentry_code"] == "RETOUCH")].copy()

            
            dataframe["reassigned_leads"] = dataframe.apply(lambda row:
                                                                    "organic" if "organik" in row["campaign_name"]
                                                                    else "organic" if "organic" in row["campaign_name"]
                                                                    else "aplikasi kasir" if "appksr" in row["campaign_name"]
                                                                    else "activity june" if row["submit_at"].month_name() == "June"
                                                                    else "activity may" if row["submit_at"].month_name() == "May"
                                                                    else "activity july", axis=1
                                            )
            # product_name
            dataframe["product_name"] = dataframe["mt_leads_code"].map(product_dict)

            # product deal category (regular or CB)
            dataframe["type_of_product"] = dataframe.apply(lambda row:
                                                                   "CB" if "CB" in str(row["product_name"])
                                                                    else "CB" if "Surprise" in str(row["product_name"])
                                                                    else "Null" if pd.isnull(row["product_name"])
                                                                    else "Regular", axis=1)

            return dataframe


    ############################ REASSIGNED DATAFRAME #############################

    # run function
    data_load_state = st.text('Loading data...')
    df_all_melesat = fetch_db_crm_melesat() 
    data_load_state.text('Loading data...done!')
    st.write(f"Updated at: {df_all_melesat.sort_values(by='last_update', ascending=False).iloc[0]['last_update']}")


    ############################ RETOUCHED DATAFRAME #############################
    def get_retouch_melesat():
        dataframe = df_all_melesat.loc[(df_all_melesat["total_activity"] >=2) & (df_all_melesat["status_code"] == "assigned") & (df_all_melesat["last_update"] >= "2022-07-21 17:00:00")].copy()

        return dataframe

    df_retouch_melesat = get_retouch_melesat()

    ############################ FISRT SUMMMARY = TOTAL ASSIGN and TOTAL RETOUCH
    # create columns 
    col1, col2, col3 = st.columns(3)

    col1.metric("Total Reassigned", value=format(len(df_all_melesat), ","), delta=f"{len(df_all_melesat)/len(df_all_melesat):.2%}")
    col2.metric("Total Retouched", value=format(len(df_retouch_melesat), ","), delta=f"{len(df_retouch_melesat)/len(df_all_melesat):.2%}")
    col3.metric("Gap", value=format(len(df_retouch_melesat)-len(df_all_melesat), ","), delta=f"{(len(df_retouch_melesat)-len(df_all_melesat))/len(df_all_melesat):.2%}")



    df_all_grouped = df_all_melesat.groupby("reassigned_leads")["mt_preleads_code"].count().to_frame().reset_index()

    df_retouched_grouped = df_retouch_melesat.groupby("reassigned_leads")["mt_preleads_code"].count().to_frame().reset_index()



    ######### Fisrt row --> Organic Leads
    # reassigned
    col1.metric("Reassigned Organic", value=format(int(df_all_grouped.loc[df_all_grouped["reassigned_leads"] == "organic", ["mt_preleads_code"]].values[0]), ","))

elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')