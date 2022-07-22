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


    ###################### ALL DATAFRAME
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
                                                        "paid deal" if row["m_status_code"] == "REQUESTED-PAYMENT"
                                                        else "deal" if row["m_status_code"] == "APPROVED-INVOICE"
                                                        else "paid deal" if row["m_status_code"] == "PAID"
                                                        else "deal" if row["m_status_code"] == "REQUEST-INVOICE"
                                                        else "deal" if row["m_status_code"] == "REJECTED-INVOICE"
                                                        else "paid deal" if row["m_status_code"] == "REJECTED-PAYMENT"
                                                        else "no deal", axis=1)


            # filter only campaign
            dataframe = dataframe.loc[dataframe["type"] == "campaign"].copy()
            
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
        
            __1. Paid Deal:__
            
            Status= REQUESTED-PAYMENT, PAID, REJECTED-PAYMENT
            
            __1. Deal:__
            
            Status = APPROVED-INVOICE, REQUEST-INVOICE, REJECTED-INVOICE
            
            __2. No Deal:__
            
            Status = Beyond
            
        
        """
        )
    ####################### REASSIGN DATAFRAME VERSION I
    @st.experimental_memo(ttl=1*60*60)
    def fetch_melesat():
            dtypes = {
                "mt_preleads_code": "category",
            }
            
            data = pd.read_csv("gs://lead-analytics-bucket/crm_db/telesales_melesat.csv",
                storage_options={'token': credentials}, 
                low_memory=False, 
                usecols=["mt_preleads_code"], 
                dtype=dtypes) # read data frame from csv file

            dataframe = data

            return dataframe




    ####################### REASSIGN DATAFRAME VERSION 2 (UPDATED)
    def get_retouch():
        dataframe = df.loc[(df["m_sourceentry_code"] == "RETOUCH"), ["mt_preleads_code"]].copy()
    
        return dataframe


    # list of preleads code
    df_melesat = get_retouch()


    ################# DAILY DEALS OF MAJOO MELESAT
    st.markdown("## Reassign Project")
    st.subheader("Daily Deals of Telesales Melesat")
    st.markdown(f"Total Available Data: __{len(df_melesat)}__")

    def get_deal_melesat():
        dataframe = df.loc[df["mt_preleads_code"].isin(df_melesat["mt_preleads_code"].tolist())].copy()

        return dataframe


    df_deal_melesat = get_deal_melesat()

    df_deal_melesat_grouped = df_deal_melesat.groupby([pd.Grouper(key="last_update", freq="D"), "deal"])["mt_preleads_code"].count().to_frame().reset_index()


    # create filter to select dataframe (deal)
    col1, col2 = st.columns(2)

    # multiselect
    select_deal = col1.multiselect("Type of Deals", options=["deal", "paid deal", "no deal"], default=["deal", "paid deal"])

    # initiate session_state
    if "select_deal_type" not in st.session_state:
        st.session_state["select_deal_type"] = select_deal

    # filtered dataframe based on multiselect
    def get_melesat_select():
        dataframe = df_deal_melesat_grouped.loc[df_deal_melesat_grouped["deal"].isin(select_deal)].copy()

        return dataframe

    df_deal_melesat_grouped_filtered = get_melesat_select()


    ####### CHART
    daily_deal_fig = px.line (df_deal_melesat_grouped_filtered, x="last_update",
        title=f"Daily Number of {st.session_state.select_deal_type}", y="mt_preleads_code", text="mt_preleads_code", color="deal", color_discrete_map={
                    "deal": "#006400",
                    "paid deal": "gold",
                    "no deal": "#6495ED"
                })
    daily_deal_fig.update_traces(textposition="bottom right", texttemplate='%{text:,}')
    daily_deal_fig.update_layout({
    'plot_bgcolor': 'rgba(0, 0, 0, 0)',
    'paper_bgcolor': 'rgba(0, 0, 0, 0)',
    })
    st.plotly_chart(daily_deal_fig, use_container_width=True)

    # create column
    col_sum1, col_sum2, col_sum3 = st.columns(3)

    ################## VALUE METRICES

    # card 1 (Paid deal)
    deal_grouped = df_deal_melesat_grouped.loc[df_deal_melesat_grouped["deal"] == "deal"].groupby("deal")["mt_preleads_code"].sum()
    total_deal = deal_grouped.values[0]
    col_sum1.metric("Total Deal", total_deal)

    # card 2 (Paid deal)
    paid_deal_grouped = df_deal_melesat_grouped.loc[df_deal_melesat_grouped["deal"] == "paid deal"].groupby("deal")["mt_preleads_code"].sum()

    def get_paid_deal():
        if len(paid_deal_grouped) > 0:
            return paid_deal_grouped.values[0]
        else:
            return 0

    total_paid_deal = get_paid_deal()
    col_sum2.metric("Total Paid Deal", total_paid_deal)

    # card 3 (All Paid + deal)
    total_all = total_deal + total_paid_deal
    col_sum3.metric("Total Paid Deal + Deal", total_all)


    ################## PERCENTAGE METRICES
    # card 1 (deal conversion rate)
    total_deal_conversion = total_deal/len(df_deal_melesat)
    col_sum1.metric("Deal Conversion Rate", value=f"{total_deal_conversion:.2%}")

    # card 2 (paid deal conversion rate)
    paid_deal_conversion = total_paid_deal/len(df_deal_melesat)
    col_sum2.metric("Paid Conversion Rate", value=f"{paid_deal_conversion:.2%}")

    # card 3 (total deal conversion rate)
    total_all_conversion = total_all/len(df_deal_melesat)
    col_sum3.metric("All Deal Conversion Rate", value=f"{total_all_conversion:.2%}")


    ############## SUN BURST DEAL, CAMPAIGN, and STATUS
    st.subheader("Number of Deals Based On Status and Campaign")

    def get_melesat_status():
        dataframe = df_deal_melesat.loc[df_deal_melesat["deal"].isin(select_deal)].copy()

        return dataframe

    df_melesat_status = get_melesat_status()

    df_melesat_status_grouped = df_melesat_status.groupby(["deal", "campaign_name", "m_status_code"])["mt_preleads_code"].count().to_frame().reset_index()

    fig_status = px.sunburst(df_melesat_status_grouped, path=["deal", "campaign_name", "m_status_code"],
                        title="Number of Deals Based on Status and Campaign", color_discrete_sequence=px.colors.qualitative.Pastel2,
                    values='mt_preleads_code', width=500, height=500)
    fig_status.update_traces(textinfo="label+percent parent")
    st.plotly_chart(fig_status, use_container_width=True)


    ############## SUN BURST DEAL, TELESALES, and STATUS
    st.subheader("Number of Deals Based On Performing Telesales")


    df_melesat_performer = df_melesat_status.groupby(["deal", "email_sales"])["mt_preleads_code"].count().to_frame().reset_index()
    df_melesat_performer = df_melesat_performer.loc[df_melesat_performer["mt_preleads_code"] >= 1].copy()
    df_melesat_performer = df_melesat_performer.sort_values(by="mt_preleads_code", ascending=True)



    ###### Session State (Slider) #####
    num_of_top = st.selectbox("Select Top Performers", options=range(1,len(df_melesat_performer)+1), index=9)

    # initiate session_state
    if "top_performer" not in st.session_state:
        st.session_state["top_performer"] = num_of_top

    change_top_performer  = st.button("Change Number Performers")

    if change_top_performer:
        st.session_state["top_performer"] = num_of_top

    ####### create columns
    col_top1, col_top2 = st.columns(2)


    ##### TOP DEAL
    def get_top_deal():
        dataframe = df_melesat_performer.loc[df_melesat_performer["deal"] == "deal"].copy()
        dataframe = dataframe.tail(st.session_state.top_performer)

        return dataframe

    top_deal_df = get_top_deal()

    top_deal_fig = go.Figure(data=[go.Bar(
                x=top_deal_df["mt_preleads_code"], y=top_deal_df["email_sales"],
                text=top_deal_df["mt_preleads_code"], orientation="h",
                textposition='auto',marker_color='LightBlue', marker_line_color='rgb(8,48,107)'
            )])

    top_deal_fig.update_traces(texttemplate='%{text:,}')
    top_deal_fig.update_layout({
    'plot_bgcolor': 'rgba(0, 0, 0, 0)',
    'paper_bgcolor': 'rgba(0, 0, 0, 0)',
    })
    col_top1.markdown(f"__Best {st.session_state.top_performer} Performers in Deal__")
    col_top1.plotly_chart(top_deal_fig, use_container_width=True)



    ##### PAID DEAL
    def get_top_paid():
        dataframe = df_melesat_performer.loc[df_melesat_performer["deal"] == "paid deal"].copy()
        dataframe = dataframe.tail(st.session_state.top_performer)

        return dataframe

    top_paid_df = get_top_paid()

    top_paid_fig = go.Figure(data=[go.Bar(
                x=top_paid_df["mt_preleads_code"], y=top_paid_df["email_sales"],
                text=top_paid_df["mt_preleads_code"], orientation="h",
                textposition='auto',marker_color='darkorange', marker_line_color='#000000'
            )])

    top_paid_fig.update_traces(texttemplate='%{text:,}')
    top_paid_fig.update_layout({
    'plot_bgcolor': 'rgba(0, 0, 0, 0)',
    'paper_bgcolor': 'rgba(0, 0, 0, 0)',
    })

    col_top2.markdown(f"__Best {st.session_state.top_performer} Performers in Paid Deal__")
    col_top2.plotly_chart(top_paid_fig, use_container_width=True)





    ####################### NON REASSIGN (REGULAR) DATAFRAME
    st.markdown("## Daily Basis")
    st.subheader("Ongoing Daily Deals (Non Reassign)")



    ########################### DAILY LINE CHART

    ###### DATAFRAME
    def get_deal_regular():
        dataframe = df.loc[~df["mt_preleads_code"].isin(df_melesat["mt_preleads_code"].tolist())].copy()

        return dataframe


    df_deal_regular = get_deal_regular()

    st.dataframe(df_deal_regular)

    df_deal_regular_grouped = df_deal_regular.groupby([pd.Grouper(key="last_update", freq="D"), "deal"])["mt_preleads_code"].count().to_frame().reset_index()


    # create filter to select dataframe (deal)
    colreg1, colreg2 = st.columns(2)

    # multiselect
    select_deal_regular = colreg1.multiselect("Type of Deals", options=["deal", "paid deal", "no deal"], default=["deal", "paid deal"], key="regular")

    # select Month
    month_name = colreg2.selectbox("Select Month", options=["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], index=6)


    # initiate session_state
    if "select_deal_type_regular" not in st.session_state:
        st.session_state["select_deal_type_regular"] = select_deal_regular

    if "select_month_name" not in st.session_state:
        st.session_state["select_month_name"] = month_name

    change_regular  = st.button("Change the filters")

    if change_regular:
        st.session_state["select_deal_type_regular"] = select_deal_regular
        st.session_state["select_month_name"] = month_name

    # filtered dataframe based on multiselect
    def get_regular_select():
        dataframe = df_deal_regular_grouped.loc[(df_deal_regular_grouped["deal"].isin(select_deal_regular)) & (df_deal_regular_grouped["last_update"].dt.month_name() == st.session_state.select_month_name)].copy()

        return dataframe

    df_deal_regular_grouped_filtered = get_regular_select()

    ####### CHART
    daily_regular_fig = px.line (df_deal_regular_grouped_filtered, x="last_update",
        title=f"Daily Number of {st.session_state.select_deal_type_regular}", y="mt_preleads_code", text="mt_preleads_code", color="deal", color_discrete_map={
                    "deal": "#006400",
                    "paid deal": "gold",
                    "no deal": "#6495ED"
                })
    daily_regular_fig.update_traces(textposition="bottom right", texttemplate='%{text:,}')
    daily_regular_fig.update_layout({
    'plot_bgcolor': 'rgba(0, 0, 0, 0)',
    'paper_bgcolor': 'rgba(0, 0, 0, 0)',
    })
    st.plotly_chart(daily_regular_fig, use_container_width=True)

    ##################################### METRIC CARDS
    def get_available_data_regular():
        dataframe = df_deal_regular.loc[df_deal_regular["last_update"].dt.month_name() == st.session_state.select_month_name].copy()

        return dataframe

    total_available_regular = len(get_available_data_regular())

    st.markdown(f"Total Available Data (Non Reassign): __{total_available_regular}__")

    # create column
    col_reg1, col_reg2, col_reg3 = st.columns(3)

    ################## VALUE METRICES

    # card 1 (Paid deal)
    deal_grouped_regular = df_deal_regular_grouped_filtered.loc[df_deal_regular_grouped_filtered["deal"] == "deal"].groupby("deal")["mt_preleads_code"].sum()
    total_deal_regular = deal_grouped_regular.values[0]
    col_reg1.metric("Total Deal", total_deal_regular)

    # card 2 (Paid deal)
    paid_deal_grouped_regular = df_deal_regular_grouped_filtered.loc[df_deal_regular_grouped_filtered["deal"] == "paid deal"].groupby("deal")["mt_preleads_code"].sum()

    def get_paid_deal_regular():
        if len(paid_deal_grouped_regular) > 0:
            return paid_deal_grouped_regular.values[0]
        else:
            return 0

    total_paid_deal_regular = get_paid_deal_regular()
    col_reg2.metric("Total Paid Deal", total_paid_deal_regular)

    # card 3 (All Paid + deal)
    total_all_regular = total_deal_regular + total_paid_deal_regular
    col_reg3.metric("Total Paid Deal + Deal", total_all_regular)


    ################## PERCENTAGE METRICES
    # card 1 (deal conversion rate)
    total_deal_conversion_regular = total_deal_regular/total_available_regular
    col_reg1.metric("Deal Conversion Rate", value=f"{total_deal_conversion_regular:.2%}")

    # card 2 (paid deal conversion rate)
    paid_deal_conversion_regular = total_paid_deal_regular/total_available_regular
    col_reg2.metric("Paid Conversion Rate", value=f"{paid_deal_conversion_regular:.2%}")

    # card 3 (total deal conversion rate)
    total_all_conversion_regular = total_all/total_available_regular
    col_reg3.metric("All Deal Conversion Rate", value=f"{total_all_conversion_regular:.2%}")


    ############## SUN BURST DEAL, SOUCRE ENTRY, and STATUS
    st.subheader("Number of Deals Based On Status and Lead Sources")

    ###### DATAFRAME
    def get_regular_status():
        dataframe = df_deal_regular.loc[(df_deal_regular["deal"].isin(select_deal_regular)) & (df_deal_regular["last_update"].dt.month_name() == st.session_state.select_month_name)].copy()
        
        return dataframe

    df_regular_status = get_regular_status()

    df_regular_status_grouped = df_regular_status.groupby(["deal", "m_sourceentry_code", "m_status_code"])["mt_preleads_code"].count().to_frame().reset_index()

    fig_source_regular = px.sunburst(df_regular_status_grouped, path=["deal", "m_sourceentry_code", "m_status_code"],
                        title=f"Number of Deals of {st.session_state.select_month_name}", color_discrete_sequence=px.colors.qualitative.Pastel2,
                    values='mt_preleads_code', width=500, height=500)
    fig_source_regular.update_traces(textinfo="label+percent parent")
    st.plotly_chart(fig_source_regular, use_container_width=True)


    ############################## END OF CONTENT





elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')

