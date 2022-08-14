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


st.set_page_config(page_title="Leads Handling", page_icon="🤸")
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
    
    st.markdown("# Leads Handling")

    
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
            # hw
            dataframe["hw"] = dataframe.apply(lambda row:
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

            # deal or no deal
            dataframe["deal"] = dataframe.apply(lambda row: 
                                                        "deal" if "PAYMENT" in str(row["m_status_code"])
                                                        else "pipeline" if "INVOICE" in str(row["m_status_code"])
                                                        else "deal" if row["m_status_code"] == "PAID"
                                                        else "leads", axis=1)

            
            # pipeline
            dataframe["pipeline_by_activity"] = dataframe.apply(lambda row:
                                                            "Pipeline Hot" if "INVOICE" in str(row["m_status_code"])
                                                            else "Pipeline Hot" if row["leads_potensial_category"] == "Hot Leads"
                                                            else "Pipeline Warm" if row["total_activity"] >=2
                                                            else "Pipeline Cold" if row["total_activity"] <=1
                                                            else "Pipeline Null", axis=1)

            

            # filter only campaign
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

    # range of submit and last update
    df_assigned["submit_update_in_days"] = (df_assigned["last_update"] - df_assigned["submit_at"])//np.timedelta64(1,"D")

    # total retouched
    df_retouched = df_assigned.loc[
        (df_assigned["m_status_code"].isin(["APPROVED-INVOICE", "FOLLOW-UP", "PAID", "REJECTED-INVOICE", "REJECTED-PAYMENT", "REQUEST-INVOICE", "REQUESTED-PAYMENT"])) &
        (df_assigned["total_activity"] >= st.session_state["assigned_act_num"])
    ].copy()

    # total rejected
    df_rejected = df_assigned.loc[
        df_assigned["m_status_code"] == "REJECTED-LEADS"
    ].copy()

    # total new
    df_new = df_assigned.loc[
        df_assigned["m_status_code"] == "NEW"
    ].copy()

    # deal
    df_deal = df_assigned.loc[
        df_assigned["deal"] == "deal"
    ].copy()

    # pipeline
    df_pipeline = df_assigned.loc[
        df_assigned["deal"] == "pipeline"
    ].copy()

    # hw
    df_hw = df_assigned.loc[
        df_assigned["hw"] == "hw"
    ].copy()



    ############################ FISRT SUMMMARY METRIC = TOTAL ASSIGN and TOTAL RETOUCH
    # create columns 
    col_met1, col_met2, col_met3, col_met4, col_met5 = st.columns(5)

    len_assigned = len(df_assigned)
    len_retouched = len(df_retouched)
    len_rejected = len(df_rejected)
    len_new = len(df_new)
    len_deal = len(df_deal)
    len_pipeline = len(df_pipeline)
    len_hw = len(df_hw)

    col_met1.metric("Total assigned", value=format(len_assigned, ","), delta=f"{len_assigned/len_assigned:.2%}", help="All leads assigned to telesales")
    col_met2.metric("Total retouched", value=format(len_retouched, ","), delta="{:.2%}".format(len_retouched/len_assigned), help=f"Num of leads followed up more than { st.session_state.assigned_act_num} activities")
    col_met3.metric("Total rejected", value=format(len_rejected, ","), delta=f"{len_rejected/len_assigned:.2%}", help="Num of rejected leads")
    col_met4.metric("Total new", value=format(len_new, ","), delta=f"{len_new/len_assigned:.2%}", help="Num of new leads (no follow-up yet)")
    col_met5.metric("Total gap", value=format(len_retouched + len_rejected + len_new - len_assigned, ","), delta=f"{(len_retouched + len_rejected + len_new - len_assigned)/len_assigned:.2%}", help="Num of leads left as follow up")
    
    # average number of activities per lead
    avg_num1, avg_num2 = st.columns(2)
    avg_num1.markdown(f" Current average number of activities per lead: __{df_assigned['total_activity'].mean():.2f}__")
    avg_num2.markdown(f" All-time average number of deal activities: __{df.loc[df['deal'] == 'deal']['total_activity'].mean():.2f}__")

    ############################ SECOND SUMMMARY METRIC = LEAD CONVERSION
    # section title
    st.subheader("Lead-to-deal Conversion")
    st.markdown("Based on __Submit Date__")
    
    # create columns 
    col_conversion1, col_conversion2, col_conversion3, col_conversion4, col_conversion5 = st.columns(5)

    col_conversion1.metric("Total assigned", value=format(len_assigned, ","), delta=f"{len_assigned/len_assigned:.2%}", help="All leads assigned to telesales")
    col_conversion2.metric("Total HW", value=format(len_hw, ","), delta="{:.2%}".format(len_hw/len_assigned), help="Number of assigned HW")
    col_conversion3.metric("Total Pipeline", value=format(len_pipeline, ","), delta="{:.2%}".format(len_pipeline/len_assigned), help="Pipeline is all status containing invoice process")
    col_conversion4.metric("Total Deal", value=format(len_deal, ","), delta="{:.2%}".format(len_deal/len_assigned), help="Deal is all status containing payment process")
    col_conversion5.metric("Deal Conversion", value="{:.2%}".format(len_deal/len_assigned), help="Num of deal divided by total assigned")

    ####################### ASSIGNED and RETOUCHED DATAFRAME BASED ON SOURCE ########################
    # section title
    st.subheader("Assigned and Retouched Based on Sources")
    st.markdown("Based on __Submit Date__")
    
    tab_campaign1, tab_campaign2, tab_campaign3 = st.tabs(["Campaign Tag", "Main Campaign", "Data Source"])

    with tab_campaign1:
        st.markdown("__Campaign Tag__")

        # assigned dataframe
        df_assigned_tag = df_assigned.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_assigned_tag.columns = ["campaign_tag", "assigned_leads"]
        
        # retouched dataframe
        df_retouched_tag = df_retouched.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_retouched_tag.columns = ["campaign_tag", "retouched_leads"]

        # rejected dataframe
        df_rejected_tag = df_rejected.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_rejected_tag.columns = ["campaign_tag", "rejected_leads"]

        # new dataframe
        df_new_tag = df_new.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_new_tag.columns = ["campaign_tag", "new_leads"]

        # hw dataframe
        df_hw_tag = df_hw.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_hw_tag.columns = ["campaign_tag", "hw"]

        # pipeline dataframe
        df_pipeline_tag = df_pipeline.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_pipeline_tag.columns = ["campaign_tag", "pipeline"]

        # deal dataframe
        df_deal_tag = df_deal.groupby("campaign_tag")["mt_preleads_code"].count().to_frame().reset_index()
        df_deal_tag.columns = ["campaign_tag", "deal"]

        # campaign_tag dataframe
        df_campaign_tag = pd.merge(df_assigned_tag, df_retouched_tag, how="left", left_on="campaign_tag", right_on="campaign_tag").merge(df_rejected_tag, how="left", right_on="campaign_tag", left_on="campaign_tag").merge(df_new_tag, how="left", right_on="campaign_tag", left_on="campaign_tag").fillna(0)
        df_campaign_tag = pd.merge(df_campaign_tag, df_hw_tag, how="left", left_on="campaign_tag", right_on="campaign_tag").merge(df_pipeline_tag, how="left", right_on="campaign_tag", left_on="campaign_tag").merge(df_deal_tag, how="left", left_on="campaign_tag", right_on="campaign_tag").fillna(0)
        df_campaign_tag["gap_leads"] = df_campaign_tag["assigned_leads"] - df_campaign_tag["retouched_leads"] - df_campaign_tag["rejected_leads"] - df_campaign_tag["new_leads"]
        df_campaign_tag["retouched_leads"] = df_campaign_tag["retouched_leads"].astype("int")
        df_campaign_tag["rejected_leads"] = df_campaign_tag["rejected_leads"].astype("int")
        df_campaign_tag["new_leads"] = df_campaign_tag["new_leads"].astype("int")
        df_campaign_tag["hw"] = df_campaign_tag["hw"].astype("int")
        df_campaign_tag["pipeline"] = df_campaign_tag["pipeline"].astype("int")
        df_campaign_tag["deal"] = df_campaign_tag["deal"].astype("int")
        df_campaign_tag["conversion"] = df_campaign_tag["deal"] / df_campaign_tag["assigned_leads"]
        df_campaign_tag['conversion'] = df_campaign_tag['conversion'].apply(lambda x: "{:.2%}".format(x))

        # reorder columns
        df_campaign_tag = df_campaign_tag.iloc[: , [0,1,2,3,4,8,5,6,7,9]]

        # ag grid
        grid_df_campaign_tag = AgGrid(df_campaign_tag, editable=True, key="1")
        new_df_campaign_tag = grid_df_campaign_tag["data"]

        
    
    with tab_campaign2:
        st.markdown("__Main Campaign__")
        
        # assigned dataframe
        df_assigned_main = df_assigned.groupby("main_campaign")["mt_preleads_code"].count().to_frame().reset_index()
        df_assigned_main.columns = ["main_campaign", "assigned_leads"]

        # retouched dataframe
        df_retouched_main = df_retouched.groupby("main_campaign")["mt_preleads_code"].count().to_frame().reset_index()
        df_retouched_main.columns = ["main_campaign", "retouched_leads"]

        # rejected dataframe
        df_rejected_main = df_rejected.groupby("main_campaign")["mt_preleads_code"].count().to_frame().reset_index()
        df_rejected_main.columns = ["main_campaign", "rejected_leads"]

        # new dataframe
        df_new_main = df_new.groupby("main_campaign")["mt_preleads_code"].count().to_frame().reset_index()
        df_new_main.columns = ["main_campaign", "new_leads"]

        # main campaign dataframe
        df_campaign_main = pd.merge(df_assigned_main, df_retouched_main, how="left", left_on="main_campaign", right_on="main_campaign").merge(df_rejected_main, how="left", right_on="main_campaign", left_on="main_campaign").merge(df_new_main, how="left", right_on="main_campaign", left_on="main_campaign").fillna(0)
        df_campaign_main["retouched_leads"] = df_campaign_main["retouched_leads"].astype("int")
        df_campaign_main["rejected_leads"] = df_campaign_main["rejected_leads"].astype("int")
        df_campaign_main["new_leads"] = df_campaign_main["new_leads"].astype("int")
        df_campaign_main["gap_leads"] = df_campaign_main["assigned_leads"] - df_campaign_main["retouched_leads"] - df_campaign_main["rejected_leads"] - df_campaign_main["new_leads"]

        # ag grid
        grid_df_campaign_main = AgGrid(df_campaign_main, editable=True, key="2")
        new_df_campaign_main = grid_df_campaign_main["data"]


    with tab_campaign3:
        st.markdown("__Data Source__")

        # assigned dataframe
        df_assigned_source = df_assigned.groupby("m_sourceentry_code")["mt_preleads_code"].count().to_frame().reset_index()
        df_assigned_source.columns = ["m_sourceentry_code", "assigned_leads"]

        # retouched dataframe
        df_retouched_source = df_retouched.groupby("m_sourceentry_code")["mt_preleads_code"].count().to_frame().reset_index()
        df_retouched_source.columns = ["m_sourceentry_code", "retouched_leads"]

        # rejected dataframe
        df_rejected_source = df_rejected.groupby("m_sourceentry_code")["mt_preleads_code"].count().to_frame().reset_index()
        df_rejected_source.columns = ["m_sourceentry_code", "rejected_leads"]

        # new dataframe
        df_new_source = df_new.groupby("m_sourceentry_code")["mt_preleads_code"].count().to_frame().reset_index()
        df_new_source.columns = ["m_sourceentry_code", "new_leads"]

        # data source dataframe
        df_campaign_source = pd.merge(df_assigned_source, df_retouched_source, how="left", left_on="m_sourceentry_code", right_on="m_sourceentry_code").merge(df_rejected_source, how="left", right_on="m_sourceentry_code", left_on="m_sourceentry_code").merge(df_new_source, how="left", right_on="m_sourceentry_code", left_on="m_sourceentry_code")
        df_campaign_source["retouched_leads"] = df_campaign_source["retouched_leads"].astype("int")
        df_campaign_source["rejected_leads"] = df_campaign_source["rejected_leads"].astype("int")
        df_campaign_source["new_leads"] = df_campaign_source["new_leads"].astype("int")
        df_campaign_source["gap_leads"] = df_campaign_source["assigned_leads"] - df_campaign_source["retouched_leads"] - df_campaign_source["rejected_leads"] - df_campaign_source["new_leads"]

        # ag grid
        grid_df_campaign_source = AgGrid(df_campaign_source, editable=True, key="3")
        new_df_campaign_source = grid_df_campaign_source["data"]

    ####################### ASSIGNED and RETOUCHED DATAFRAME BASED ON SOURCE ########################
    # section title
    st.subheader("Assigned and Retouched Based on Tele's Name")
    st.markdown("Based on __Submit Date__")

    tab_tele1, tab_tele2, tab_tele3, tab_tele4 = st.tabs(["Full Name", "Campaign Tag", "Actvities", "Submit-Last Update Range"])
    
    with tab_tele1:
        st.markdown("__Tele's Name__")

        # assigned dataframe
        df_assigned_tele_tag = df_assigned.groupby("full_name")["mt_preleads_code"].count().to_frame().reset_index()
        df_assigned_tele_tag.columns = ["full_name", "assigned_leads"]
        
        # retouched dataframe
        df_retouched_tele_tag = df_retouched.groupby("full_name")["mt_preleads_code"].count().to_frame().reset_index()
        df_retouched_tele_tag.columns = ["full_name", "retouched_leads"]

        # rejected dataframe
        df_rejected_tele_tag = df_rejected.groupby("full_name")["mt_preleads_code"].count().to_frame().reset_index()
        df_rejected_tele_tag.columns = ["full_name", "rejected_leads"]

        # new dataframe
        df_new_tele_tag = df_new.groupby("full_name")["mt_preleads_code"].count().to_frame().reset_index()
        df_new_tele_tag.columns = ["full_name", "new_leads"]

        # hw dataframe
        df_hw_tele_tag = df_hw.groupby("full_name")["mt_preleads_code"].count().to_frame().reset_index()
        df_hw_tele_tag.columns = ["full_name", "hw"]

        # pipeline dataframe
        df_pipeline_tele_tag = df_pipeline.groupby("full_name")["mt_preleads_code"].count().to_frame().reset_index()
        df_pipeline_tele_tag.columns = ["full_name", "pipeline"]

        # deal dataframe
        df_deal_tele_tag = df_deal.groupby("full_name")["mt_preleads_code"].count().to_frame().reset_index()
        df_deal_tele_tag.columns = ["full_name", "deal"]


        # campaign_tag by tele dataframe
        df_campaign_tele_tag = pd.merge(df_assigned_tele_tag, df_retouched_tele_tag, how="left", left_on="full_name", right_on="full_name").merge(df_rejected_tele_tag, how="left", right_on="full_name", left_on="full_name").merge(df_new_tele_tag, how="left", right_on="full_name", left_on="full_name")
        df_campaign_tele_tag = pd.merge(df_campaign_tele_tag, df_hw_tele_tag, how="left", left_on="full_name", right_on="full_name").merge(df_pipeline_tele_tag, how="left", left_on="full_name", right_on="full_name").merge(df_deal_tele_tag, how="left", left_on="full_name", right_on="full_name")
        df_campaign_tele_tag["gap_leads"] = df_campaign_tele_tag["assigned_leads"] - df_campaign_tele_tag["retouched_leads"] - df_campaign_tele_tag["rejected_leads"] - df_campaign_tele_tag["new_leads"]
        df_campaign_tele_tag["conversion"] = df_campaign_tele_tag["deal"] / df_campaign_tele_tag["assigned_leads"]
        df_campaign_tele_tag['conversion'] = df_campaign_tele_tag['conversion'].replace(np.nan, 0)
        df_campaign_tele_tag['conversion'] = df_campaign_tele_tag['conversion'].apply(lambda x: "{:.2%}".format(x))
        
        # reorder columns
        df_campaign_tele_tag = df_campaign_tele_tag.iloc[: , [0,1,2,3,4,8,5,6,7,9]]
        
        # ag grid
        grid_df_campaign_tele_tag = AgGrid(df_campaign_tele_tag, editable=True, key="4")
        new_df_campaign_tele_tag = grid_df_campaign_tele_tag["data"]
        


    with tab_tele2:
        st.markdown("__Campaign Tag__")

        # selection
        select_df_campaign_tag = st.selectbox("Select data", options=["Assigned", "Retouched", "New", "Rejected"], index=0)

        # SESSION STATE
        if "select_data_campaign_tag" not in st.session_state:
         st.session_state["select_data_campaign_tag"] = select_df_campaign_tag

        # button to update state
        change_params_campaign_tag = st.button("Change the data")

        # update the state
        if change_params_campaign_tag:
            st.session_state["select_data_campaign_tag"] = select_df_campaign_tag

        def select_dataframe_campaign_tag(value=st.session_state.select_data_campaign_tag):
            if value == "Assigned":
                return df_assigned

            elif value == "Retouched":
                return df_retouched
            
            elif value == "Rejected":
                return df_rejected
            else:
                return df_new

        df_used_campaign_tag = select_dataframe_campaign_tag()

        df_used_campaign_tag_grouped = df_used_campaign_tag.groupby(["full_name", "campaign_tag"])["mt_preleads_code"].count().to_frame().reset_index()
        df_used_campaign_tag_pivot = pd.pivot_table(df_used_campaign_tag_grouped, index=["full_name"], columns=["campaign_tag"], values="mt_preleads_code", aggfunc=np.sum)
        df_used_campaign_tag_pivot.reset_index(inplace=True)

        # ag grid
        grid_df_used_campaign_tag = AgGrid(df_used_campaign_tag_pivot, editable=True, key="5")
        new_df_used_campaign_tele_tag = grid_df_used_campaign_tag["data"]


    with tab_tele3:
        st.markdown("__Activities__")
        st.markdown("_Average num of activities of All Assigned leads_")

        df_avg_activity = df_assigned.groupby("full_name")["total_activity"].mean().to_frame().reset_index()
        df_avg_activity.columns = ["full_name", "average_activity"]
        df_avg_activity['average_activity'] = df_avg_activity['average_activity'].replace(np.nan, 0)
        df_avg_activity['average_activity'] = df_avg_activity['average_activity'].apply(lambda x: "{0:.2f}".format(x))
        

        # ag grid
        grid_avg_activity = AgGrid(df_avg_activity, editable=True, key="6")
        new_grid_avg_activity = grid_avg_activity["data"]

    with tab_tele4:
        st.markdown("__Submit-Last Update Range__")
        st.markdown("_Average difference between submit and last update of All Assigned leads (in days)_")

        df_avg_difference = df_assigned.groupby("full_name")["submit_update_in_days"].mean().to_frame().reset_index()
        df_avg_difference.columns = ["full_name", "diff_submit_update_in_days"]
        df_avg_difference["diff_submit_update_in_days"] = df_avg_difference["diff_submit_update_in_days"].replace(np.nan, 0)
        df_avg_difference["diff_submit_update_in_days"] = df_avg_difference["diff_submit_update_in_days"].apply(lambda x: "{0:.2f}".format(x))

        # ag grid
        grid_avg_difference = AgGrid(df_avg_difference, editable=True, key="7")
        new_grid_avg_difference = grid_avg_difference["data"]

    ############################## END OF CONTENT
    


elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')

