import pandas as pd
from sklearn.preprocessing import LabelEncoder
import numpy as np
import requests
import json
import os
import mysql.connector as mysql
from mysql.connector import Error
import plotly.graph_objects as go
import streamlit as st
import plotly.express as px
conn = mysql.connect(host='localhost', user='root',password='12345')
cursor=conn.cursor()
cursor.execute('USE phonepepulseexp')
cursor.execute("SELECT State, Quater, SUM(Transaction_amount) as Total_Transaction_amount FROM Aggregate_transactions1 GROUP BY State, Quater")


filtered_rows = cursor.fetchall()


df = pd.DataFrame(filtered_rows, columns=["State", "Quater", "Transaction_amount"])

conn.commit()


state_trans = st.selectbox("Select State", df["State"].unique())


df = df[df["State"] == state_trans]


fig = px.bar(df, x="Quater", y="Transaction_amount", color="State")


st.plotly_chart(fig)




cursor.execute('USE phonepepulseexp')
cursor.execute("SELECT State, Quater, SUM( User_Brand and Brand_Count) as Brand FROM Aggregate_Users GROUP BY State, Quater")

# Fetch the filtered rows
filtered_rows = cursor.fetchall()

# Create a DataFrame from the filtered rows
df = pd.DataFrame(filtered_rows, columns=["State", "Quater", "Brand"])

conn.commit()

# Create a dropdown menu using Streamlit
state_user = st.selectbox("Select State", df["State"].unique())

# Filter the DataFrame based on the selected state
df1 = df[df["State"] == state_user]

# Create a bar chart using Plotly Express
fig1 = px.bar(df, x="Quater", y="Brand", color="State")

# Display the bar chart using Streamlit
st.plotly_chart(fig1)

cursor.execute("SELECT State, Year, Quater, District, SUM(Reg_users) as Total_Users FROM Map_Users1  GROUP BY State, Year, Quater, District, Reg_users")

# Fetch the filtered rows
filtered_rows = cursor.fetchall()

# Create a DataFrame from the filtered rows
df = pd.DataFrame(filtered_rows, columns=["State", "Year", "Quater", "District", "Reg_users"])

# Create a dropdown menu using Streamlit
#state = st.selectbox("Select State", df["State"].unique())
state_filter1 = st.selectbox("Select State", ["All", *df['State'].unique()])
year_filter1 = st.selectbox("Select Year", ["All", *df['Year'].unique()])
quater_filter1 = st.selectbox("Select Quater", ["All", *df['Quater'].unique()])
district_filter1 = st.selectbox("Select District", ["All", *df['District'].unique()])


if state_filter1 != "All":
    df = df[df['State'] == state_filter1]
if year_filter1 != "All":
    df = df[df['Year'] == int(year_filter1)]
if quater_filter1 != "All":
    df = df[df['Quater'] == quater_filter1]
if district_filter1 != "All":
    df = df[df['District'] == district_filter1]

#df = df[(df['Users'] >= Total_Users[0]) & (df['Users'] <= Total_Users[1])]

cursor.execute("SELECT State, Quater, Year, District , SUM(Amount) as total_amount FROM Top_transactions GROUP BY State, Year, Quater, District, Amount")

# Fetch the filtered rows
filtered_rows = cursor.fetchall()

# Create a DataFrame from the filtered rows
df = pd.DataFrame(filtered_rows, columns=["State", "Quater", "Year", "District", "Amount"])

# Create a dropdown menu using Streamlit
#state = st.selectbox("Select State", df["State"].unique())
state_filter2 = st.selectbox("Select State", ["All", *df['State'].unique()])
year_filter2 = st.selectbox("Select Year", ["All", *df['Year'].unique()])
district_filter2 = st.selectbox("Select District", ["All", *df['District'].unique()])
quater_filter2 = st.selectbox("Select Quater", ["All", *df['Quater'].unique()])
#total_amount = st.slider("Amount", 0, 9000000, (0, 9000000))

if state_filter2 != "All":
    df = df[df['State'] == state_filter2]
if year_filter2 != "All":
    df = df[df['Year'] == int(year_filter2)]
if quater_filter2 != "All":
    df = df[df['Quater'] == quater_filter2]
if district_filter2 != "All":
    df = df[df['District'] == district_filter2]

#df = df[(df['Amount'] >= total_amount[0]) & (df['Amount'] <= total_amount[1])]


# create table
st.write(df)


cursor.execute("SELECT State, Year, Quater, District_name, SUM( Reguser) as Total_Users FROM Top_Users GROUP BY State, Year, Quater, District_name,  Reguser")

# Fetch the filtered rows
filtered_rows = cursor.fetchall()

# Create a DataFrame from the filtered rows
df = pd.DataFrame(filtered_rows, columns=["State", "Year", "Quater", "District_name", " Reguser"])

# Create a dropdown menu using Streamlit
#state = st.selectbox("Select State", df["State"].unique())
state_filter3 = st.selectbox("Select State", ["All", *df['State'].unique()])
year_filter3 = st.selectbox("Select Year", ["All", *df['Year'].unique()])
quater_filter3 = st.selectbox("Select Quater", ["All", *df['Quater'].unique()])
district_filter3 = st.selectbox("Select District_name", ["All", *df['District_name'].unique()])


if state_filter3 != "All":
    df = df[df['State'] == state_filter3]
if year_filter3 != "All":
    df = df[df['Year'] == int(year_filter3)]
if quater_filter3 != "All":
    df = df[df['Quater'] == quater_filter3]
if district_filter3 != "All":
    df = df[df['District_name'] == district_filter3]

#df = df[(df['Users'] >= Total_Users[0]) & (df['Users'] <= Total_Users[1])]


# create table
st.write(df)
df = pd.read_csv("Agg_trans.csv")

fig = px.choropleth(
    df,
    geojson="Agg_trans.csv",
    featureidkey='properties.ST_NM',
    locations='State',
    color='Transaction_amount',
    color_continuous_scale='Reds'
)

fig.update_geos(fitbounds="locations", visible=False)

fig.show()
     



df = pd.read_csv("https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/active_cases_2020-07-17_0800.csv")

fig = go.Figure(data=go.Choropleth(
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locationmode='geojson-id',
    locations=df['state'],
    z=df['active cases'],

    autocolorscale=False,
    colorscale='Reds',
    marker_line_color='peachpuff',

    colorbar=dict(
        title={'text': "Active Cases"},

        thickness=15,
        len=0.35,
        bgcolor='rgba(255,255,255,0.6)',

        tick0=0,
        dtick=20000,

        xanchor='left',
        x=0.01,
        yanchor='bottom',
        y=0.05
    )
))

fig.update_geos(
    visible=False,
    projection=dict(
        type='conic conformal',
        parallels=[12.472944444, 35.172805555556],
        rotation={'lat': 24, 'lon': 80}
    ),
    lonaxis={'range': [68, 98]},
    lataxis={'range': [6, 38]}
)

fig.update_layout(
    title=dict(
        text="Active COVID-19 Cases in India by State as of July 17, 2020",
        xanchor='center',
        x=0.5,
        yref='paper',
        yanchor='bottom',
        y=1,
        pad={'b': 10}
    ),
    margin={'r': 0, 't': 30, 'l': 0, 'b': 0},
    height=550,
    width=550
)

fig.show()
     









