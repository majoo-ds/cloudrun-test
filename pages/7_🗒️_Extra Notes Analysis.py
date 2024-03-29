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
import json
import ast


st.set_page_config(page_title="Extra Notes", page_icon="📝")
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
    
    st.markdown("# Extra Notes Analysis")

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

            # campaign_tag
            dataframe["campaign_tag"] = dataframe["campaign_name"].map(campaign_tag_dict)

            # main_campaign
            dataframe["main_campaign"] = dataframe["campaign_name"].map(main_campaign_dict)

            # remove duplicates
            dataframe.drop_duplicates(subset=["mt_preleads_code"], inplace=True)

            return dataframe

    # run function
    data_load_state = st.text('Loading data...')
    df = fetch_db_crm_monitoring() 
    data_load_state.text('Loading data...done!')
    st.write(f"Updated at: {df.sort_values(by='last_update', ascending=False).iloc[0]['last_update']}")

    # section title
    st.subheader("Extra Notes Extraction of Leads")
    st.markdown("Based on __Submit Date__")

    # selection widget
    col_notes1, col_notes2 = st.columns(2)
    # leads type category
    date_start_note = col_notes1.date_input("Select start date", value=datetime.datetime.today().replace(day=1), help="Based on submit at")
    date_end_note = col_notes2.date_input("Select end date", value=datetime.datetime.today(), help="Based on submit at")
    

    # SESSION STATE
    if "note_start_date" not in st.session_state:
        st.session_state["note_start_date"] = date_start_note

    if "note_end_date" not in st.session_state:
        st.session_state["note_end_date"] = date_end_note


    # button to update state
    change_date = st.button("Change filter")

    # update the state
    if change_date:
        st.session_state["note_start_date"] = date_start_note
        st.session_state["note_end_date"] = date_end_note

    # total assigned
    df_filtered = df.loc[
        (df["type"] == "campaign") &
        (df["submit_at"].dt.date >= st.session_state["note_start_date"]) &
        (df["submit_at"].dt.date <= st.session_state["note_end_date"])
    ].copy()


    ######################## DATA MANIPULATION #########################
    # main dataframe
    df_extra_note = df_filtered.loc[(df_filtered["extra_note"] != "null") & (df_filtered["extra_note"].notnull())].copy()
    # slice of main dataframe
    df_submit = df_extra_note.loc[:, ["submit_at", "mt_preleads_code", "leads_potensial_category",
                                          "deal", "hw", "main_campaign", "campaign_tag"]].copy()


    # normalize the column of extra_note
    json_notes = json.loads(df_extra_note[["mt_preleads_code", "extra_note"]].to_json(orient="records"))    
    df_clean_extra_note = pd.json_normalize(json_notes)

    # FIRST EXTRACTION
    df_cleaned_grab_marketplace = pd.merge(df_clean_extra_note, df_submit, how="left", left_on="mt_preleads_code", right_on="mt_preleads_code")

    # SECOND EXTRACTION
    df_extra_notes_2 = df_cleaned_grab_marketplace.loc[df_cleaned_grab_marketplace["extra_note"].notnull()].copy()
    
    def none_to_empty_str(items):
        return {k: v if v is not None else '' for k, v in items}
    
    df_extra_notes_2["extra_note"] = df_extra_notes_2["extra_note"].apply(lambda x: json.dumps(x))
    df_extra_notes_2["extra_note"] = df_extra_notes_2["extra_note"].apply(lambda x: json.loads(x, object_pairs_hook=none_to_empty_str))
    
    null = None
    
    df_extra_notes_2["extra_note"] = df_extra_notes_2["extra_note"].apply(lambda x: eval(x))
    json_notes2 = json.loads(df_extra_notes_2.to_json(orient="records"))    
    df_clean_extra_note2 = pd.json_normalize(json_notes2)
    df_cleaned_note = pd.merge(df_clean_extra_note2, df_submit, how="left", left_on="mt_preleads_code", right_on="mt_preleads_code")
    

    # THIRD EXTRACTION
    df_extra_notes_3 = df_cleaned_note.loc[df_cleaned_note["extra_note"].notnull()].copy()
    df_extra_notes_3["extra_note"] = df_extra_notes_3["extra_note"].apply(lambda x: eval(x))
    # normalize the column order_transaction
    json_notes3 = json.loads(df_extra_notes_3[["mt_preleads_code", "extra_note"]].to_json(orient="records"))    
    df_clean_extra_note3 = pd.json_normalize(json_notes3)
    # final clean dataframe
    df_extra_cleaned_note = pd.merge(df_clean_extra_note3, df_submit, how="left", left_on="mt_preleads_code", right_on="mt_preleads_code")
    df_extra_cleaned_note.drop_duplicates(subset=["mt_preleads_code"], inplace=True)


    ############################ TABS OF EXTRA NOTES #########################
    tab_note1, tab_note2, tab_note3, tab_note4 = st.tabs(["Interesting Features", "Installation Time", "Preferred Contact", "Business Development"])

    with tab_note1:
        st.markdown("__Interesting Feature__")
        
        # main dataframe
        df_interesting_feature = df_extra_cleaned_note["extra_note.interesting_features"].value_counts().to_frame().reset_index()
        df_interesting_feature["_%"] = df_interesting_feature["extra_note.interesting_features"]/sum(df_interesting_feature["extra_note.interesting_features"])
        df_interesting_feature["_%"] = df_interesting_feature["_%"].apply(lambda x: "{0:.2f}%".format(x*100))
        df_interesting_feature.columns = ["feature", "count", "%_of_count"]

        # hw dataframe
        warm_interesting_feature = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.interesting_features"].notnull()) &
                         (df_extra_cleaned_note["leads_potensial_category"] == "Warm Leads")].groupby("extra_note.interesting_features")["mt_preleads_code"].count().to_frame().reset_index()
        warm_interesting_feature.columns = ["feature", "warm_count"]
        
        hot_interesting_feature = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.interesting_features"].notnull()) &
                         (df_extra_cleaned_note["leads_potensial_category"] == "Hot Leads")].groupby("extra_note.interesting_features")["mt_preleads_code"].count().to_frame().reset_index()
        hot_interesting_feature.columns = ["feature", "hot_count"]

        # deal dataframe
        deal_interesting_feature = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.interesting_features"].notnull()) &
                         (df_extra_cleaned_note["deal"] == "deal")].groupby("extra_note.interesting_features")["mt_preleads_code"].count().to_frame().reset_index()
        deal_interesting_feature.columns = ["feature", "deal_count"]

        # campaign_tag dataframe
        campaign_tag_interesting_feature = df_extra_cleaned_note.loc[df_extra_cleaned_note["extra_note.interesting_features"].notnull()].groupby(["extra_note.interesting_features", "campaign_tag"])["mt_preleads_code"].count().to_frame().reset_index()
        campaign_tag_interesting_feature.columns = ["feature", "campaign_tag", "campaign_count"]
        campaign_tag_interesting_feature = campaign_tag_interesting_feature.pivot(index="feature", columns="campaign_tag", values="campaign_count")
        campaign_tag_interesting_feature.fillna(0, inplace=True)
        
        # final dataframe
        df_interesting_feature_final = pd.merge(df_interesting_feature, warm_interesting_feature, how="left", left_on="feature", right_on="feature").merge(
                                               hot_interesting_feature, how="left", left_on="feature", right_on="feature" 
                                            ).merge(
                                                deal_interesting_feature, how="left", left_on="feature", right_on="feature"
                                            ).merge(
                                                campaign_tag_interesting_feature, how="left", left_on="feature", right_on="feature"
                                            )
        df_interesting_feature_final.fillna(0, inplace=True)
        df_interesting_feature_final["deal_conversion"] = df_interesting_feature_final["deal_count"] / df_interesting_feature_final["count"]
        df_interesting_feature_final["deal_conversion"] = df_interesting_feature_final["deal_conversion"].apply(lambda x: "{0:.2f}%".format(x*100))

        # ag grid
        grid_df_interesting_feature = AgGrid(df_interesting_feature_final, editable=True, key="0")
        new_df_campaign_tag = grid_df_interesting_feature["data"]


    with tab_note2:
        st.markdown("__Installation Time__")

        # main dataframe
        df_installation_time = df_extra_cleaned_note["extra_note.installation_time"].value_counts().to_frame().reset_index()
        df_installation_time["_%"] = df_installation_time["extra_note.installation_time"]/sum(df_installation_time["extra_note.installation_time"])
        df_installation_time["_%"] = df_installation_time["_%"].apply(lambda x: "{0:.2f}%".format(x*100))
        df_installation_time.columns = ["installation", "count", "%_of_count"]

        # hw dataframe
        warm_installation = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.installation_time"].notnull()) &
                         (df_extra_cleaned_note["leads_potensial_category"] == "Warm Leads")].groupby("extra_note.installation_time")["mt_preleads_code"].count().to_frame().reset_index()
        warm_installation.columns = ["installation", "warm_count"]

        hot_installation = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.installation_time"].notnull()) &
                         (df_extra_cleaned_note["leads_potensial_category"] == "Hot Leads")].groupby("extra_note.installation_time")["mt_preleads_code"].count().to_frame().reset_index()
        hot_installation.columns = ["installation", "hot_count"]

        # deal dataframe
        deal_installation = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.installation_time"].notnull()) &
                         (df_extra_cleaned_note["deal"] == "deal")].groupby("extra_note.installation_time")["mt_preleads_code"].count().to_frame().reset_index()
        deal_installation.columns = ["installation", "deal_count"]

        # campaign_tag dataframe
        campaign_tag_installation = df_extra_cleaned_note.loc[df_extra_cleaned_note["extra_note.installation_time"].notnull()].groupby(["extra_note.installation_time", "campaign_tag"])["mt_preleads_code"].count().to_frame().reset_index()
        campaign_tag_installation.columns = ["installation", "campaign_tag", "campaign_count"]
        campaign_tag_installation = campaign_tag_installation.pivot(index="installation", columns="campaign_tag", values="campaign_count")
        campaign_tag_installation.fillna(0, inplace=True)
        
        # final dataframe
        df_installation_final = pd.merge(df_installation_time, warm_installation, how="left", left_on="installation", right_on="installation").merge(
                                                hot_installation, how="left", left_on="installation", right_on="installation"
                                            ).merge(
                                                deal_installation, how="left", left_on="installation", right_on="installation"
                                            ).merge(
                                                campaign_tag_installation, how="left", left_on="installation", right_on="installation")
        df_installation_final.fillna(0, inplace=True)
        df_installation_final["deal_conversion"] = df_installation_final["deal_count"] / df_installation_final["count"]
        df_installation_final["deal_conversion"] = df_installation_final["deal_conversion"].apply(lambda x: "{0:.2f}%".format(x*100))

        # ag grid
        grid_df_installation_final = AgGrid(df_installation_final, editable=True, key="1")
        new_df_campaign_tag = grid_df_installation_final["data"]


    with tab_note3:
        st.markdown("__Preferred Contact__")

        # main dataframe
        df_contact = df_extra_cleaned_note["extra_note.prefered_contact"].value_counts().to_frame().reset_index()
        df_contact["_%"] = df_contact["extra_note.prefered_contact"]/sum(df_contact["extra_note.prefered_contact"])
        df_contact["_%"] = df_contact["_%"].apply(lambda x: "{0:.2f}%".format(x*100))
        df_contact.columns = ["contact", "count", "%_of_count"]

        # hw dataframe
        warm_contact = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.prefered_contact"].notnull()) &
                         (df_extra_cleaned_note["leads_potensial_category"] == "Warm Leads")].groupby("extra_note.prefered_contact")["mt_preleads_code"].count().to_frame().reset_index()
        warm_contact.columns = ["contact", "warm_count"]
        hot_contact = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.prefered_contact"].notnull()) &
                         (df_extra_cleaned_note["leads_potensial_category"] == "Hot Leads")].groupby("extra_note.prefered_contact")["mt_preleads_code"].count().to_frame().reset_index()
        hot_contact.columns = ["contact", "hot_count"]

        # deal dataframe
        deal_contact = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.prefered_contact"].notnull()) &
                         (df_extra_cleaned_note["deal"] == "deal")].groupby("extra_note.prefered_contact")["mt_preleads_code"].count().to_frame().reset_index()
        deal_contact.columns = ["contact", "deal_count"]

        # campaign_tag dataframe
        campaign_tag_contact = df_extra_cleaned_note.loc[df_extra_cleaned_note["extra_note.prefered_contact"].notnull()].groupby(["extra_note.prefered_contact", "campaign_tag"])["mt_preleads_code"].count().to_frame().reset_index()
        campaign_tag_contact.columns = ["contact", "campaign_tag", "campaign_count"]
        campaign_tag_contact = campaign_tag_contact.pivot(index="contact", columns="campaign_tag", values="campaign_count")
        campaign_tag_contact.fillna(0, inplace=True)

        # final dataframe
        df_contact_final = pd.merge(df_contact, warm_contact, how="left", left_on="contact", right_on="contact").merge(
                                        hot_contact, how="left", left_on="contact", right_on="contact"
                                    ).merge(
                                        deal_contact, how="left", left_on="contact", right_on="contact"
                                    ).merge(
                                        campaign_tag_contact, how="left", left_on="contact", right_on="contact")
        df_contact_final.fillna(0, inplace=True)
        df_contact_final["deal_conversion"] = df_contact_final["deal_count"] / df_contact_final["count"]
        df_contact_final["deal_conversion"] = df_contact_final["deal_conversion"].apply(lambda x: "{0:.2f}%".format(x*100))
        
        # ag grid
        grid_df_contact_final = AgGrid(df_contact_final, editable=True, key="2")
        new_df_campaign_tag = grid_df_contact_final["data"]
        

    with tab_note4:
        st.markdown("__Business Development__")

        # main dataframe
        df_busdev = df_extra_cleaned_note["extra_note.business_development"].value_counts().to_frame().reset_index()
        df_busdev["_%"] = df_busdev["extra_note.business_development"]/sum(df_busdev["extra_note.business_development"])
        df_busdev["_%"] = df_busdev["_%"].apply(lambda x: "{0:.2f}%".format(x*100))
        df_busdev.columns = ["busdev", "count", "%_of_count"]
        
        # hw dataframe
        warm_busdev = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.business_development"].notnull()) &
                         (df_extra_cleaned_note["leads_potensial_category"] == "Warm Leads")].groupby("extra_note.business_development")["mt_preleads_code"].count().to_frame().reset_index()
        warm_busdev.columns = ["busdev", "warm_count"]
        hot_busdev = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.business_development"].notnull()) &
                         (df_extra_cleaned_note["leads_potensial_category"] == "Hot Leads")].groupby("extra_note.business_development")["mt_preleads_code"].count().to_frame().reset_index()
        hot_busdev.columns = ["busdev", "hot_count"]

        
        # deal dataframe
        deal_busdev = df_extra_cleaned_note.loc[(df_extra_cleaned_note["extra_note.business_development"].notnull()) &
                         (df_extra_cleaned_note["deal"] == "deal")].groupby("extra_note.business_development")["mt_preleads_code"].count().to_frame().reset_index()
        deal_busdev.columns = ["busdev", "deal_count"]

        # campaign_tag dataframe
        campaign_tag_busdev = df_extra_cleaned_note.loc[df_extra_cleaned_note["extra_note.business_development"].notnull()].groupby(["extra_note.business_development", "campaign_tag"])["mt_preleads_code"].count().to_frame().reset_index()
        campaign_tag_busdev.columns = ["busdev", "campaign_tag", "campaign_count"]
        campaign_tag_busdev = campaign_tag_busdev.pivot(index="busdev", columns="campaign_tag", values="campaign_count")
        campaign_tag_busdev.fillna(0, inplace=True)

        # final dataframe
        df_busdev_final = pd.merge(df_busdev, warm_busdev, how="left", left_on="busdev", right_on="busdev").merge(
                                        hot_busdev, how="left", left_on="busdev", right_on="busdev"
                                    ).merge(
                                        deal_busdev, how="left", left_on="busdev", right_on="busdev"
                                    ).merge(
                                        campaign_tag_busdev, how="left", left_on="busdev", right_on="busdev")
        df_busdev_final.fillna(0, inplace=True)
        df_busdev_final["deal_conversion"] = df_busdev_final["deal_count"] / df_busdev_final["count"]
        df_busdev_final["deal_conversion"] = df_busdev_final["deal_conversion"].apply(lambda x: "{0:.2f}%".format(x*100))

        # ag grid
        grid_df_busdev_final = AgGrid(df_busdev_final, editable=True, key="3")
        new_df_campaign_tag = grid_df_busdev_final["data"]

############################## END OF CONTENT
    


elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')