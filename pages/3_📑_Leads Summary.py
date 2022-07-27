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

st.set_page_config(page_title="Leads Summary", page_icon="🔑")
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
    st.markdown("# Leads Summary")

    # bucket name
    bucket_name ="lead-analytics-bucket"

    ####################### PRODUCT DATAFRAME
    @st.experimental_memo(ttl=1*60*60)
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


    @st.experimental_memo(ttl=1*60*60)
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
            dataframe = dataframe.loc[dataframe["type"] == "campaign"].copy()

            
            # product_name
            dataframe["product_name"] = dataframe["mt_leads_code"].map(product_dict)

            # product deal category (regular or CB)
            dataframe["type_of_product"] = dataframe.apply(lambda row:
                                                                   "CB" if "CB" in str(row["product_name"])
                                                                    else "CB" if "Surprise" in str(row["product_name"])
                                                                    else "Null" if pd.isnull(row["product_name"])
                                                                    else "Regular", axis=1)

            return dataframe

    # run function
    data_load_state = st.text('Loading data...')
    df_all = fetch_db_crm_1() 
    data_load_state.text('Loading data...done!')
    st.write(f"Updated at: {df_all.sort_values(by='last_update', ascending=False).iloc[0]['last_update']}")

    grid_return = AgGrid(df_all.head(50), editable=True)
    new_df = grid_return["data"]

    ################################ TOTAL LEADS GENERATED #########################################
    # section title
    st.subheader("Total Generated Leads (All Status)")
    st.markdown("Based on __Submit Date__")

    
    # options
    # col_tot1, col_tot2, col_tot3 = st.columns([2,2,3])
    # year_total = col_tot1.selectbox("Select Year", options=["2022", "2023", "2024", "2025"], index=0)
    status_list = st.multiselect("Select/Unselect Status", options=["assigned", "unassigned", "junked", "backlog"], default=["assigned", "unassigned", "junked", "backlog"], help="Filter number of leads based on status. Default is all selected")
    # # SESSION STATE
    # if "year_only" not in st.session_state:
    #     st.session_state["year_only"] = year_total

    if "status_touch" not in st.session_state:
         st.session_state["status_touch"] = status_list


    # # button to update state
    change_params = st.button("Change the filters")

    # update the state
    if change_params:
        st.session_state["status_touch"] = status_list

    # graph title
    st.markdown(f"Monthly Total Leads ({datetime.datetime.now().year})")
    first_touch_total = df_all.loc[(df_all["submit_at"].dt.year == int(datetime.datetime.now().year)) & (df_all["status_code"].isin(st.session_state.status_touch))].groupby(pd.Grouper(key="submit_at", freq="MS"))["mt_preleads_code"].count().to_frame().reset_index()

    total_leads_monthly_fig = go.Figure(data=[go.Bar(
                x=first_touch_total["submit_at"], y=first_touch_total["mt_preleads_code"],
                text=first_touch_total["mt_preleads_code"],
                textposition='auto',marker_color='LightBlue', marker_line_color='rgb(8,48,107)'
            )])

    total_leads_monthly_fig.update_traces(texttemplate='%{text:,}')
    total_leads_monthly_fig.update_layout({
    'plot_bgcolor': 'rgba(0, 0, 0, 0)',
    'paper_bgcolor': 'rgba(0, 0, 0, 0)',
    })
    st.plotly_chart(total_leads_monthly_fig, use_container_width=True)
    
    ################################ BACKLOG JUNKED ASSIGNED UNASSIGNED #########################################
    # section title
    st.subheader("First Touch of Generated Leads")
    st.markdown("Based on __Submit Date__")

    status1, status2, status3 = st.columns([3,3,2])
    month_group = status1.selectbox("Select Month", options=["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], index=6)
    year_group = status2.selectbox("Select Year", options=["2022", "2023", "2024", "2025"], index=0)

    # SESSION STATE
    if "month_status" not in st.session_state:
        st.session_state["month_status"] = month_group

    if "year_status" not in st.session_state:
        st.session_state["year_status"] = year_group

    # button to update state
    change_year_month = st.button("Change Pie Chart")

    # update the state
    if change_year_month:
        st.session_state["month_status"] = month_group
        st.session_state["year_status"] = year_group

    # create function to filter
    def get_leads_first_touch():
        df = df_all.loc[(df_all["submit_at"].dt.month_name() == st.session_state["month_status"]) & (df_all["submit_at"].dt.year == int(st.session_state["year_status"]))].copy() 
        return df

    # DATAFRAME
    first_touch_df = get_leads_first_touch()


    groupby_status_first_touch_df = first_touch_df.groupby("status_code")["mt_preleads_code"].count().to_frame().reset_index()
    color_pie = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']

    # DONUT CHART (ALTERNATIVE OF PIE CHART)
    donut_status_fig = go.Figure(data=[go.Pie(labels=groupby_status_first_touch_df["status_code"], values=groupby_status_first_touch_df["mt_preleads_code"], hole=.35)])
    donut_status_fig.update_traces(textposition='inside', textinfo='percent+label', textfont_size=12, marker=dict(colors=color_pie, line=dict(color='#000000', width=1.25)))
    donut_status_fig.update_layout(title_text=f"Processed Leads of {st.session_state.month_status} {st.session_state.year_status}")
    st.plotly_chart(donut_status_fig, use_container_width=True)


    ################################ BACKLOG JUNKED ASSIGNED UNASSIGNED OVER THE PERIOD #########################################
    # section title
    st.subheader("Number of Touched Leads Over The Monthly Period")
    st.markdown("Based on __Submit Date__")
    # dataframe
    first_touch_monthly = df_all.loc[df_all["submit_at"].dt.year == int(datetime.datetime.now().year)].groupby([pd.Grouper(key="submit_at", freq="MS"), "status_code"])["mt_preleads_code"].count().to_frame().reset_index()
    # figure
    monthly_first_status_fig = px.line (first_touch_monthly, x="submit_at",
        title=f"Year of {datetime.datetime.now().year}", y="mt_preleads_code", text="mt_preleads_code", color="status_code", color_discrete_map={
                    "assigned": "#006400",
                    "junked": "gold",
                    "backlog": "#6495ED",
                    "unassigned": "#FF8C00"
                })
    monthly_first_status_fig.update_traces(textposition="bottom right", texttemplate='%{text:,}')
    monthly_first_status_fig.update_layout({
    'plot_bgcolor': 'rgba(0, 0, 0, 0)',
    'paper_bgcolor': 'rgba(0, 0, 0, 0)',
    })
    st.plotly_chart(monthly_first_status_fig, use_container_width=True)


    ################################ LEADS TARGET VS REALIZATION #########################################
    st.subheader("Target Achievement of Generated Leads")
    st.markdown("Based on __Submit Date__")

    # target dataframe
    url_csv = "https://docs.google.com/spreadsheets/d/e/2PACX-1vShdJ-CyjIH0b4vvbIB-aDYPdKdtDExaMskHeY7qARDrBPPXpo8MpR9Rer7FQRfNzWR3QzBd-w_fO1Q/pub?output=csv"

    @st.experimental_memo(ttl=24*60*60)
    def get_target(path):
        df = pd.read_csv(path, parse_dates=["period"])

        return df

    # import target dataframe using get_target function
    target_df = get_target(url_csv)
    

    # selection widget
    col_tar1, col_tar2, col_tar3, col_tar4 = st.columns(4)

    # month
    target_month = col_tar1.selectbox("Select Month", options=["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], index=6, key="target")
    # year
    target_year = col_tar2.selectbox("Select Year", options=["2022", "2023", "2024", "2025"], index=0, key="target")
    # type
    target_type = col_tar3.selectbox("Select Type", options=["total leads", "hw"], index=1)
    # method
    target_method = col_tar4.selectbox("Select Type", options=["rating", "activity"], index=1, help="Categorizing leads based on rating value or num of activity")

    # SESSION STATE
    if "month_target" not in st.session_state:
        st.session_state["month_target"] = target_month

    if "year_target" not in st.session_state:
        st.session_state["year_target"] = target_year

    if "year_type" not in st.session_state:
        st.session_state["year_type"] = target_type

    if "year_method" not in st.session_state:
        st.session_state["year_method"] = target_method


    # button to update state
    change_target = st.button("Change filters")

    # update the state
    if change_target:
        st.session_state["month_target"] = target_month
        st.session_state["year_target"] = target_year
        st.session_state["year_type"] = target_type
        st.session_state["year_method"] = target_method

    df_target_filtered = target_df.loc[(target_df["period"].dt.month_name() == st.session_state.month_target) &
                                    (target_df["period"].dt.year == int(st.session_state.year_target)) &
                                    (target_df["type"] == st.session_state.year_type)].copy()

    # st.dataframe(df_target_filtered)
    # st.write(df_target_filtered.iat[0,2])
   

    # leads dataframe
    #################################### BY RATING
    # groupby number of preleads
    hw_leads = df_all.loc[(df_all["submit_at"].dt.month_name() == st.session_state.month_target) &
                                    (df_all["submit_at"].dt.year == int(st.session_state.year_target)) &
                                    (df_all["leads_potensial_category"] != "Null")].groupby([pd.Grouper(key="submit_at", freq="MS"), "leads_potensial_category"])["mt_preleads_code"].count().to_frame().reset_index()
    # long to wide format
    hw_leads_wide = pd.pivot(hw_leads, index="submit_at", columns="leads_potensial_category", values="mt_preleads_code").reset_index()

    #################################### BY NUM OF ACTIVITIES
    # groupby number of preleads
    hw_leads_act = df_all.loc[(df_all["submit_at"].dt.month_name() == st.session_state.month_target) &
                                    (df_all["submit_at"].dt.year == int(st.session_state.year_target)) &
                                    (df_all["leads_potensial_category"] != "Null")].groupby([pd.Grouper(key="submit_at", freq="MS"), "pipeline_by_activity"])["mt_preleads_code"].count().to_frame().reset_index()
    # long to wide format
    hw_leads_wide_act = pd.pivot(hw_leads_act, index="submit_at", columns="pipeline_by_activity", values="mt_preleads_code").reset_index()

    # selected leads dataframe
    def selected_lead_dataframe(value=st.session_state.year_method):
        if value == "rating":
            return hw_leads_wide
        else:
            return hw_leads_wide_act

    df_leads_filtered = selected_lead_dataframe()
    
    
    # selected type (accumulation)
    # if total leads = sum(h+c+w)
    def selected_type_dataframe(value=st.session_state.year_type):
        if value == "hw":
            return df_leads_filtered.iat[0,2] + df_leads_filtered.iat[0,3]
        else:
            return df_leads_filtered.iat[0,1] + df_leads_filtered.iat[0,2] + df_leads_filtered.iat[0,3]

    # realization number (actual number)
    realization_num = selected_type_dataframe()
    

    # function to round up to nearest thousands
    def round_up_to_nearest_1000(num):
        return math.ceil(num / 1000) * 1000

    # 50% of realization
    first_range = 0.5*df_target_filtered.iat[0,2]
    # 90% of realization
    num_threshold = 0.9*df_target_filtered.iat[0,2]
    # 75% of realization
    second_range = 0.75*df_target_filtered.iat[0,2]


    # color legend
    col_color1, col_color2, col_color3, col_color4, col_color5 = st.columns(5)
    with col_color1:
        color1 = st.color_picker("", '#FFD700', key=1)
        st.markdown(f"50% of target: __{first_range:,}__")

    with col_color2:
        color2 = st.color_picker("", '#FF8C00', key=2)
        st.markdown(f"75% of target: __{second_range:,}__")

    with col_color3:
        color3 = st.color_picker("", '#ADD8E6', key=3)
        st.markdown(f"90% of target: __{num_threshold:,}__")

    with col_color4:
        color4 = st.color_picker("", '#000000', key=4)
        st.markdown(f"100% of target: __{df_target_filtered.iat[0,2]:,}__")

    with col_color5:
        color5 = st.color_picker("", '#778899', key=5)
        st.markdown(f"Actual numbers: __{realization_num:,}__")

    # bullet chart
    bullet_target_fig = go.Figure()

    bullet_target_fig.add_trace(go.Indicator(
        mode = "number+gauge+delta", value = realization_num,
        delta = {'reference': df_target_filtered.iat[0,2]},
        domain = {'x': [0.25, 1], 'y': [0.7, 0.9]},
        title = {'text': f"{st.session_state.year_type.upper()}"},
        gauge = {
            'shape': "bullet",
            'axis': {'range': [None, round_up_to_nearest_1000(df_target_filtered.iat[0,2])]},
            'threshold': {
                'line': {'color': color4, 'width': 2.5},
                'thickness': 0.75,
                'value': df_target_filtered.iat[0,2]},
            'steps': [
                {'range': [0, first_range], 'color': color1},
                {'range': [first_range, second_range], 'color': color2},
                {'range': [second_range, num_threshold], 'color': color3}],
            'bar': {'color': color5}}))
    bullet_target_fig.update_layout(height = 300 , margin = {'t':0, 'b':0, 'l':0})        
    st.plotly_chart(bullet_target_fig, use_container_width=True)

    ############################## CONTENT END HERE ############################################





elif st.session_state["authentication_status"] == False:
    st.error('Username/password is incorrect')

elif st.session_state["authentication_status"] == None:
    st.warning('Please enter your username and password')