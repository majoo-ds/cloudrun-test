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
    st.subheader("Available Data")
    st.markdown("#### All Reassigned")
    # create columns 
    col1, col2, col3 = st.columns(3)
    
    col1.metric("Total Reassigned", value=format(len(df_all_melesat), ","), delta=f"{len(df_all_melesat)/len(df_all_melesat):.2%}")
    col2.metric("Total Retouched", value=format(len(df_retouch_melesat), ","), delta=f"{len(df_retouch_melesat)/len(df_all_melesat):.2%}")
    col3.metric("Total Gap", value=format(len(df_retouch_melesat)-len(df_all_melesat), ","), delta=f"{(len(df_retouch_melesat)-len(df_all_melesat))/len(df_all_melesat):.2%}")

    # dataframe

    df_all_grouped = df_all_melesat.groupby("reassigned_leads")["mt_preleads_code"].count().to_frame().reset_index()

    df_retouched_grouped = df_retouch_melesat.groupby("reassigned_leads")["mt_preleads_code"].count().to_frame().reset_index()

    ######### First row --> Organic Leads
    st.markdown("#### Organic")
    col_org1, col_org2, col_org3 = st.columns(3)
    # reassigned
    reassigned_organic = int(df_all_grouped.loc[df_all_grouped["reassigned_leads"] == "organic", ["mt_preleads_code"]].values[0])
    col_org1.metric("Reassigned Organic", value=format(reassigned_organic, ","), delta=f"{(reassigned_organic)/(reassigned_organic):.2%}")

    # retouched
    retouched_organic = int(df_retouched_grouped.loc[df_retouched_grouped["reassigned_leads"] == "organic", ["mt_preleads_code"]].values[0])
    col_org2.metric("Retouched Organic", value=format(retouched_organic, ","), delta=f"{(retouched_organic)/(reassigned_organic):.2%}")

    # gap
    col_org3.metric("Organic Gap", value=format(retouched_organic-reassigned_organic, ","), delta=f"{(retouched_organic-reassigned_organic)/(reassigned_organic):.2%}")


    ######### Second row --> App Kasir Leads
    st.markdown("#### Aplikasi Kasir")
    col_ksr1, col_ksr2, col_ksr3 = st.columns(3)

    # reassigned
    reassigned_kasir = int(df_all_grouped.loc[df_all_grouped["reassigned_leads"] == "aplikasi kasir", ["mt_preleads_code"]].values[0])
    col_ksr1.metric("Reassigned App kasir", value=format(reassigned_kasir, ","), delta=f"{(reassigned_kasir)/(reassigned_kasir):.2%}")

    # retouched
    retouched_kasir = int(df_retouched_grouped.loc[df_retouched_grouped["reassigned_leads"] == "aplikasi kasir", ["mt_preleads_code"]].values[0])
    col_ksr2.metric("Retouched App Kasir", value=format(retouched_kasir, ","), delta=f"{(retouched_kasir)/(reassigned_kasir):.2%}")

    # gap
    col_ksr3.metric("App Kasir Gap", value=format(retouched_kasir-reassigned_kasir, ","), delta=f"{(retouched_kasir-reassigned_kasir)/(reassigned_kasir):.2%}")


    ######### Third row --> Activity May
    st.markdown("#### Activity May")
    col_may1, col_may2, col_may3 = st.columns(3)


    # reassigned
    reassigned_may = int(df_all_grouped.loc[df_all_grouped["reassigned_leads"] == "activity may", ["mt_preleads_code"]].values[0])
    col_may1.metric("Reassigned Actvity May", value=format(reassigned_may, ","), delta=f"{(reassigned_may)/(reassigned_may):.2%}")

    # retouched
    retouched_may = int(df_retouched_grouped.loc[df_retouched_grouped["reassigned_leads"] == "activity may", ["mt_preleads_code"]].values[0])
    col_may2.metric("Retouched Activity May", value=format(retouched_may, ","), delta=f"{(retouched_may)/(reassigned_may):.2%}")

    # gap
    col_may3.metric("Activity May Gap", value=format(retouched_may-reassigned_may, ","), delta=f"{(retouched_may-reassigned_may)/(reassigned_may):.2%}")

    ######### Fourth row --> Activity June
    st.markdown("#### Activity June")
    col_june1, col_june2, col_june3 = st.columns(3)


    # reassigned
    reassigned_june = int(df_all_grouped.loc[df_all_grouped["reassigned_leads"] == "activity june", ["mt_preleads_code"]].values[0])
    col_june1.metric("Reassigned Actvity June", value=format(reassigned_june, ","), delta=f"{(reassigned_june)/(reassigned_june):.2%}")
    # retouched
    retouched_june = int(df_retouched_grouped.loc[df_retouched_grouped["reassigned_leads"] == "activity june", ["mt_preleads_code"]].values[0])
    col_june2.metric("Retouched Activity June", value=format(retouched_june, ","), delta=f"{(retouched_june)/(reassigned_june):.2%}")

    # gap
    col_june3.metric("Activity June Gap", value=format(retouched_june-reassigned_june, ","), delta=f"{(retouched_june-reassigned_june)/(reassigned_june):.2%}")

    ######### Fifthrow --> Activity July
    st.markdown("#### Activity July")
    col_july1, col_july2, col_july3 = st.columns(3)


    # reassigned
    reassigned_july = int(df_all_grouped.loc[df_all_grouped["reassigned_leads"] == "activity july", ["mt_preleads_code"]].values[0])
    col_july1.metric("Reassigned Actvity July", value=format(reassigned_july, ","), delta=f"{(reassigned_july)/(reassigned_july):.2%}")
    # retouched
    retouched_july = int(df_retouched_grouped.loc[df_retouched_grouped["reassigned_leads"] == "activity july", ["mt_preleads_code"]].values[0])
    col_july2.metric("Retouched Activity July", value=format(retouched_july, ","), delta=f"{(retouched_july)/(reassigned_july):.2%}")

    # gap
    col_july3.metric("Activity July Gap", value=format(retouched_july-reassigned_july, ","), delta=f"{(retouched_july-reassigned_july)/(reassigned_july):.2%}")


   

    ############################ SECOND SUMMMARY = PIPELINE and PRODUCT TYPE ###########################################
    ############## ALL





    ################################################# RETOUCH #######################################################
    st.subheader("Conversion of Retouched Leads")
    df_retouch_pipeline_product = df_retouch_melesat.loc[df_retouch_melesat["deal"] != "leads"].groupby(["deal", "reassigned_leads", "type_of_product"])["mt_preleads_code"].count().to_frame().reset_index()


    ######### First row --> Organic Leads ##########
    st.markdown("### Organic")
    
    ####### CB
    st.markdown("#### CB")
    col_org_cb1, col_org_cb2 = st.columns(2)

    # CB Pipeline
    cb_pipeline_organic_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "organic") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()
    
    def get_cb_pipeline_product():
        if len(cb_pipeline_organic_df) > 0:
            return cb_pipeline_organic_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_pipeline_organic = get_cb_pipeline_product()      
    col_org_cb1.metric("Pipeline", value = num_cb_pipeline_organic, delta=f"Conversion: {(num_cb_pipeline_organic/retouched_organic):.2%}")
    
   
    # CB Deal
    cb_deal_organic_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "organic") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()
    
    def get_cb_deal_product():
        if len(cb_deal_organic_df) > 0:
            return cb_deal_organic_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_deal_organic = get_cb_deal_product()        
    col_org_cb2.metric("Deal", value = num_cb_deal_organic, delta=f"Conversion: {(num_cb_deal_organic/retouched_organic):.2%}")


    ####### Regular
    st.markdown("#### Regular")
    col_org_reg1, col_org_reg2 = st.columns(2)

    # regular pipeline
    reg_pipeline_organic_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "organic") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()
    
    def get_reg_pipeline_product():
        if len(reg_pipeline_organic_df) > 0:
            return reg_pipeline_organic_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_pipeline_organic = get_reg_pipeline_product()        
    col_org_reg1.metric("Pipeline", value = num_reg_pipeline_organic, delta=f"Conversion: {(num_reg_pipeline_organic/retouched_organic):.2%}")

    # Regular Deal
    reg_deal_organic_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "organic") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()

   
    def get_reg_deal_product():
        if len(reg_deal_organic_df) > 0:
            return reg_deal_organic_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_deal_organic = get_reg_deal_product()        
    col_org_reg2.metric("Deal", value = num_reg_deal_organic, delta=f"Conversion: {(num_reg_deal_organic/retouched_organic):.2%}")

    

    ######### Second row --> App Kasir Leads ##########
    st.markdown("### App Kasir")
    
    ####### CB
    st.markdown("#### CB")
    col_app_cb1, col_app_cb2 = st.columns(2)

    # CB Pipeline
    cb_pipeline_app_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "aplikasi kasir") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()

    def get_app_cb_pipeline_product():
        if len(cb_pipeline_app_df) > 0:
            return cb_pipeline_app_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_pipeline_app = get_app_cb_pipeline_product()      
    col_app_cb1.metric("Pipeline", value = num_cb_pipeline_app, delta=f"Conversion: {(num_cb_pipeline_app/retouched_kasir):.2%}")


    # CB Deal
    cb_deal_app_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "aplikasi kasir") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()
    
    def get_app_cb_deal_product():
        if len(cb_deal_app_df) > 0:
            return cb_deal_app_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_deal_app = get_app_cb_deal_product()        
    col_app_cb2.metric("Deal", value = num_cb_deal_app, delta=f"Conversion: {(num_cb_deal_app/retouched_kasir):.2%}")


    ####### Regular
    st.markdown("#### Regular")
    col_app_reg1, col_app_reg2 = st.columns(2)


    # regular pipeline
    reg_pipeline_app_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "aplikasi kasir") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()


    def get_app_reg_pipeline_product():
        if len(reg_pipeline_app_df) > 0:
            return reg_pipeline_app_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_pipeline_app = get_app_reg_pipeline_product()        
    col_app_reg1.metric("Pipeline", value = num_reg_pipeline_app, delta=f"Conversion: {(num_reg_pipeline_app/retouched_kasir):.2%}")

    # Regular Deal
    reg_deal_app_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "aplikasi kasir") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()

    def get_app_reg_deal_product():
        if len(reg_deal_app_df) > 0:
            return reg_deal_app_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_deal_app = get_app_reg_deal_product()        
    col_app_reg2.metric("Deal", value = num_reg_deal_app, delta=f"Conversion: {(num_reg_deal_app/retouched_kasir):.2%}")




    ######### Third row --> Activity May Leads ##########
    st.markdown("### Activity May")

    ####### CB
    st.markdown("#### CB")
    col_may_cb1, col_may_cb2 = st.columns(2)


    # CB Pipeline
    cb_pipeline_may_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity may") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()

    def get_may_cb_pipeline_product():
        if len(cb_pipeline_may_df) > 0:
            return cb_pipeline_may_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_pipeline_may = get_may_cb_pipeline_product()      
    col_may_cb1.metric("Pipeline", value = num_cb_pipeline_may, delta=f"Conversion: {(num_cb_pipeline_may/retouched_may):.2%}")

    # CB Deal
    cb_deal_may_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity may") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()
    
    def get_app_cb_deal_product():
        if len(cb_deal_may_df) > 0:
            return cb_deal_may_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_deal_may = get_app_cb_deal_product()        
    col_may_cb2.metric("Deal", value = num_cb_deal_may, delta=f"Conversion: {(num_cb_deal_may/retouched_may):.2%}")


    ####### Regular
    st.markdown("#### Regular")
    col_may_reg1, col_may_reg2 = st.columns(2)


    # regular pipeline
    reg_pipeline_may_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity may") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()


    def get_may_reg_pipeline_product():
        if len(reg_pipeline_may_df) > 0:
            return reg_pipeline_may_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_pipeline_may = get_may_reg_pipeline_product()        
    col_may_reg1.metric("Pipeline", value = num_reg_pipeline_may, delta=f"Conversion: {(num_reg_pipeline_may/retouched_may):.2%}")


    # Regular Deal
    reg_deal_may_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity may") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()

    def get_may_reg_deal_product():
        if len(reg_deal_may_df) > 0:
            return reg_deal_may_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_deal_may = get_may_reg_deal_product()        
    col_may_reg2.metric("Deal", value = num_reg_deal_may, delta=f"Conversion: {(num_reg_deal_may/retouched_may):.2%}")



    ######### Fourth row --> Activity June Leads ##########
    st.markdown("### Activity June")

    ####### CB
    st.markdown("#### CB")
    col_june_cb1, col_june_cb2 = st.columns(2)

    # CB Pipeline
    cb_pipeline_june_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity june") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()

    def get_june_cb_pipeline_product():
        if len(cb_pipeline_june_df) > 0:
            return cb_pipeline_june_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_pipeline_june = get_june_cb_pipeline_product()      
    col_june_cb1.metric("Pipeline", value = num_cb_pipeline_june, delta=f"Conversion: {(num_cb_pipeline_june/retouched_june):.2%}")


    # CB Deal
    cb_deal_june_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity june") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()
    
    def get_june_cb_deal_product():
        if len(cb_deal_june_df) > 0:
            return cb_deal_june_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_deal_june = get_june_cb_deal_product()        
    col_june_cb2.metric("Deal", value = num_cb_deal_june, delta=f"Conversion: {(num_cb_deal_june/retouched_june):.2%}")

    ####### Regular
    st.markdown("#### Regular")
    col_june_reg1, col_june_reg2 = st.columns(2)

    # regular pipeline
    reg_pipeline_june_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity june") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()


    def get_june_reg_pipeline_product():
        if len(reg_pipeline_june_df) > 0:
            return reg_pipeline_june_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_pipeline_june = get_june_reg_pipeline_product()        
    col_june_reg1.metric("Pipeline", value = num_reg_pipeline_june, delta=f"Conversion: {(num_reg_pipeline_june/retouched_june):.2%}")


    # Regular Deal
    reg_deal_june_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity june") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()

    def get_june_reg_deal_product():
        if len(reg_deal_june_df) > 0:
            return reg_deal_june_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_deal_june = get_june_reg_deal_product()        
    col_june_reg2.metric("Deal", value = num_reg_deal_june, delta=f"Conversion: {(num_reg_deal_june/retouched_june):.2%}")


    ######### Fifth row --> Activity July Leads ##########
    st.markdown("### Activity July")

    ####### CB
    st.markdown("#### CB")
    col_july_cb1, col_july_cb2 = st.columns(2)

    # CB Pipeline
    cb_pipeline_july_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity july") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()

    def get_july_cb_pipeline_product():
        if len(cb_pipeline_july_df) > 0:
            return cb_pipeline_july_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_pipeline_july = get_july_cb_pipeline_product()      
    col_july_cb1.metric("Pipeline", value = num_cb_pipeline_july, delta=f"Conversion: {(num_cb_pipeline_july/retouched_july):.2%}")


     # CB Deal
    cb_deal_july_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "CB") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity july") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()
    
    def get_july_cb_deal_product():
        if len(cb_deal_july_df) > 0:
            return cb_deal_july_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_cb_deal_july = get_june_cb_deal_product()        
    col_july_cb2.metric("Deal", value = num_cb_deal_july, delta=f"Conversion: {(num_cb_deal_july/retouched_july):.2%}")


    ####### Regular
    st.markdown("#### Regular")
    col_july_reg1, col_july_reg2 = st.columns(2)

    # regular pipeline
    reg_pipeline_july_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity july") &
                                                            (df_retouch_pipeline_product["deal"] == "pipeline"), ["mt_preleads_code"]].copy()


    def get_july_reg_pipeline_product():
        if len(reg_pipeline_july_df) > 0:
            return reg_pipeline_july_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_pipeline_july = get_july_reg_pipeline_product()        
    col_july_reg1.metric("Pipeline", value = num_reg_pipeline_july, delta=f"Conversion: {(num_reg_pipeline_july/retouched_july):.2%}")



    # Regular Deal
    reg_deal_july_df = df_retouch_pipeline_product.loc[(df_retouch_pipeline_product["type_of_product"] == "Regular") & 
                                                            (df_retouch_pipeline_product["reassigned_leads"] == "activity july") &
                                                            (df_retouch_pipeline_product["deal"] == "deal"), ["mt_preleads_code"]].copy()

    def get_july_reg_deal_product():
        if len(reg_deal_july_df) > 0:
            return reg_deal_july_df["mt_preleads_code"].values[0]
        else:
            return 0

    num_reg_deal_july = get_july_reg_deal_product()        
    col_july_reg2.metric("Deal", value = num_reg_deal_july, delta=f"Conversion: {(num_reg_deal_july/retouched_july):.2%}")


    ############################ THIRD SUMMMARY = HCW and PRODUCT TYPE
    st.subheader("HCW Summary")

    # create columns
    col_hcw1, col_hcw2 = st.columns(2)

    ############## ALL

    df_hcw_all = df_all_melesat.loc[df_all_melesat["type_of_product"] != "Null"].groupby(["deal","reassigned_leads", "leads_potensial_category", "type_of_product"])["mt_preleads_code"].count().to_frame().reset_index()

    fig_hcw_all = px.sunburst(df_hcw_all, path=["deal", "reassigned_leads", "leads_potensial_category", "type_of_product"],
                        title="HCW of All Reassigned Leads", color_discrete_sequence=px.colors.qualitative.Pastel2,
                    values='mt_preleads_code', width=500, height=500)
    fig_hcw_all.update_traces(textinfo="label+percent parent")
    col_hcw1.plotly_chart(fig_hcw_all, use_container_width=True)


    ############## RETOUCH

    df_hcw_retouched = df_retouch_melesat.loc[df_retouch_melesat["type_of_product"] != "Null"].groupby(["deal","reassigned_leads", "leads_potensial_category", "type_of_product"])["mt_preleads_code"].count().to_frame().reset_index()

    fig_hcw_retouch = px.sunburst(df_hcw_retouched, path=["deal", "reassigned_leads", "leads_potensial_category", "type_of_product"],
                        title="HCW of All Retouched Leads", color_discrete_sequence=px.colors.qualitative.Pastel2,
                    values='mt_preleads_code', width=500, height=500)
    fig_hcw_retouch.update_traces(textinfo="label+percent parent")
    col_hcw2.plotly_chart(fig_hcw_retouch, use_container_width=True)



elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')