import pandas as pd
from sklearn.preprocessing import LabelEncoder
import numpy as np
import requests
import json
import os
import mysql.connector as mysql
from mysql.connector import Error
import streamlit as st
import plotly.express as px

path="/phonepe/pulse/data/aggregated/transaction/country/india/state"
Agg_state=os.listdir(path)
Z={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
for i in Agg_state:
    p=path+"/"+i
    Agg_yr=os.listdir(p) 
    for row in Agg_yr:
        M=p+"/"+row
        Agg_yr=os.listdir(M)
        for k in Agg_yr:
            N=M+"/"+k
            Data=open(N,'r')
            A=json.load(Data)
            for v in A['data']['transactionData']:
                   Name=v["name"]
                   count=v["paymentInstruments"][0]['count']
                   amount=v["paymentInstruments"][0]['amount']
                   Z["Transaction_type"].append(Name)
                   Z["Transaction_count"].append(count)
                   Z["Transaction_amount"].append(amount)
                   Z["State"].append(i)
                   Z["Year"].append(row)
                   Z["Quarter"].append(int(k.strip('.json')))
df=pd.DataFrame(Z)
df.to_csv('Agg_trans.csv')

path="/phonepe/pulse/data/aggregated/user/country/india/state"
Agg_state=os.listdir(path)
Z2={'State':[], 'Year':[],'Quarter':[],'Brand':[], 'count':[], 'Percentage':[]}
for i in Agg_state:
    p=path+"/"+i
    Agg_yr=os.listdir(p) 
    for row in Agg_yr:
        M=p+"/"+row
        Agg_yr=os.listdir(M)
        for k in Agg_yr:
            N=M+"/"+k
            Data=open(N,'r')
            A=json.load(Data)
            try:
                
                for v in A['data']['usersByDevice']:
                       Z2["Brand"].append(v['brand'])
                       Z2["count"].append(v['count'])
                       Z2["Percentage"]=v['percentage']
                       Z2["State"].append(i)
                       Z2["Year"].append(row)
                       Z2["Quarter"].append(int(k.strip('.json')))
            except:
                     pass
df2=pd.DataFrame(Z2)
df2.to_csv("user_by_device.csv")


path="/phonepe/pulse/data/map/transaction/hover/country/india/state"
hover_state=os.listdir(path)
Z3={'State':[], 'Year':[],'Quarter':[],'District':[], 'Count':[], 'Amount':[]}
for i in hover_state:
    p=path+"/"+i
    hover_yr=os.listdir(p) 
    for row in hover_yr:
        M=p+"/"+row
        hover_yr=os.listdir(M)
        for k in hover_yr:
            N=M+"/"+k
            Data=open(N,'r')
            A=json.load(Data)
            for v in A['data']['hoverDataList']:
                   Name=v["name"]
                   count=v["metric"][0]['count']
                   amount=v["metric"][0]['amount']
                   Z3["District"].append(Name)
                   Z3["Count"].append(count)
                   Z3["Amount"].append(amount)
                   Z3["State"].append(i)
                   Z3["Year"].append(row)
                   Z3["Quarter"].append(int(k.strip('.json')))
df3=pd.DataFrame(Z3)
df3.to_csv("map_trans.csv")
path="/phonepe/pulse/data/map/user/hover/country/india/state"
hover_state=os.listdir(path)
Z4={'State':[], 'Year':[],'Quarter':[],'Users':[], 'Districts':[]}
for i in hover_state:
    p=path+"/"+i
    hover_yr=os.listdir(p) 
    for row in hover_yr:
        M=p+"/"+row
        hover_yr=os.listdir(M)
        for k in hover_yr:
            N=M+"/"+k
            Data=open(N,'r')
            A=json.load(Data)
            for district,values in A['data']['hoverData'].items():
                   users=values['registeredUsers']
                   d=district
                   Z4["Users"].append(users)
                   Z4["Districts"].append(d)
                   Z4["State"].append(i)
                   Z4["Year"].append(row)
                   Z4["Quarter"].append(int(k.strip('.json')))
df4=pd.DataFrame(Z4)
df4.to_csv('map_user_state.csv')

path="/phonepe/pulse/data/top/transaction/country/india/state/"
top_states=os.listdir(path)


Z5={'State':[], 'Year':[],'Quarter':[],'Districts':[], 'Count':[], 'Amount':[]}
for i in top_states:
    p=path+i+"/"
    top_yr=os.listdir(p)    
    for row in top_yr:
        M=p+"/"+row
        top_yr_list=os.listdir(M)        
        for k in top_yr_list:
            N=M+'/'+k
            with open(N, 'r') as file:
                A=json.load(file)
            for z in A['data']['districts']:
                name=z['entityName']
                count=z['metric']['count']
                amount=z['metric']['amount']
                Z5['Districts'].append(name)
                Z5['Count'].append(count)
                Z5['Amount'].append(amount)
                Z5['State'].append(i)
                Z5['Year'].append(row)
                Z5["Quarter"].append(int(k.strip('.json')))

df5=pd.DataFrame(Z5)
df5.to_csv("top_trans_state.csv")

path="/phonepe/pulse/data/top/user/country/india/state/"
top_states=os.listdir(path)
Z6={'State':[], 'Year':[],'Quarter':[],'Districts':[], 'Users':[]}
for i in top_states:
    p=path+i+"/"
    top_yr=os.listdir(p)    
    for row in top_yr:
        M=p+"/"+row
        top_yr_list=os.listdir(M)        
        for k in top_yr_list:
            N=M+'/'+k
            with open(N, 'r') as file:
                A=json.load(file)
            for z in A['data']['districts']:
                name=z['name']
                count=z['registeredUsers']
                Z6['Districts'].append(name)
                Z6['Users'].append(count)
                Z6['State'].append(i)
                Z6['Year'].append(row)
                Z6["Quarter"].append(int(k.strip('.json')))

df6=pd.DataFrame(Z6)
df6.to_csv("top_user_state.csv")

agg_trans=pd.read_csv("Agg_trans.csv")
agg_trans=agg_trans.drop(['Unnamed: 0'],axis=1)

try:
    conn = mysql.connect(host='localhost', user='root',password='12345')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE phonepepulseexp")
        print("Database is created")
except Error as e:
    print("Error while connecting to MySQL", e)

cursor.execute("Use phonepepulseexp")
cursor.execute('CREATE TABLE Aggregate_transactions1 (State VARCHAR(100), Year INT, Quater INT, Transaction_type VARCHAR(75),  Transaction_count INT,  Transaction_amount BIGINT )')
print("Table is created")

for i,row in agg_trans.iterrows():
            
            sql = "INSERT INTO phonepepulseexp.Aggregate_transactions1 VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            
           
            conn.commit()
sql = "SELECT * FROM phonepepulseexp.Aggregate_transactions1"
cursor.execute(sql)
result = cursor.fetchall()
for i in result:
    print(i)
    
agg_user= pd.read_csv("user_by_device.csv",index_col=False,delimiter = ',')

agg_user=agg_user.drop(['Unnamed: 0'],axis=1)

cursor.execute('USE phonepepulseexp')
cursor.execute('CREATE TABLE Aggregate_Users (State VARCHAR(100), Year INT, Quater INT, User_Brand VARCHAR(75),  Brand_Count BIGINT,  Brand_Percentage BIGINT )')
print("Table is created")
for i,row in agg_user.iterrows():
            
            sql = "INSERT INTO  phonepepulseexp.Aggregate_Users VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            
            
            conn.commit()
sql = "SELECT * FROM  phonepepulseexp.Aggregate_Users"
cursor.execute(sql)

result = cursor.fetchall()
for i in result:
    print(i)

map_trans= pd.read_csv('map_trans.csv',index_col=False,delimiter = ',')
map_trans=map_trans.drop(['Unnamed: 0'],axis=1)
cursor.execute('USE phonepepulseexp')
cursor.execute('CREATE TABLE Map_transactions (State VARCHAR(100), Year INT, Quater INT, Hover_area VARCHAR(75),  Hover_count INT,  Hover_amount BIGINT )')
print("Table is created")
for i,row in map_trans.iterrows():
            
            sql = "INSERT INTO phonepepulseexp.Map_transactions VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            
            
            conn.commit()
sql = "SELECT * FROM phonepepulseexp.Map_transactions"
cursor.execute(sql)

result = cursor.fetchall()
for i in result:
    print(i)

map_user= pd.read_csv('map_user_state.csv',index_col=False,delimiter = ',')

map_user=map_user.drop(['Unnamed: 0'],axis=1)

cursor.execute('USE phonepepulseexp')
cursor.execute('CREATE TABLE Map_Users1 (State VARCHAR(100), Year INT, Quater INT, District VARCHAR(75),  Reg_users VARCHAR(75) )')
print("Table is created")
for i,row in map_user.iterrows():
           
            sql = "INSERT INTO phonepepulseexp.Map_Users1 VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            
            
            conn.commit()
sql = "SELECT * FROM phonepepulseexp.Map_Users1"
cursor.execute(sql)

result = cursor.fetchall()
for i in result:
    print(i)
    
top_trans= pd.read_csv('top_trans_state.csv',index_col=False,delimiter = ',')

top_trans=top_trans.drop(['Unnamed: 0'],axis=1)

cursor.execute('USE phonepepulseexp')
cursor.execute('CREATE TABLE Top_transactions (State VARCHAR(100), Year INT, Quater INT, District VARCHAR(75), Count INT,Amount BIGINT )')
print("Table is created")
for i,row in top_trans.iterrows():
             
            sql = "INSERT INTO phonepepulseexp.Top_transactions VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
           
            conn.commit()
sql = "SELECT * FROM phonepepulseexp.Top_transactions"
cursor.execute(sql)

result = cursor.fetchall()
for i in result:
    print(i)
top_user= pd.read_csv('top_user_state.csv',index_col=False,delimiter = ',')
top_user=top_user.drop(['Unnamed: 0'],axis=1)
cursor.execute('USE phonepepulseexp')
cursor.execute('CREATE TABLE Top_Users (State VARCHAR(100), Year INT, Quater INT, District_name VARCHAR(75),  Reguser VARCHAR(15) )')
print("Table is created")
for i,row in top_user.iterrows():
           
            sql = "INSERT INTO phonepepulseexp.Top_Users VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            
          
            conn.commit()
sql = "SELECT * FROM phonepepulseexp.Top_Users"
cursor.execute(sql)

result = cursor.fetchall()
for i in result:
    print(i)

cursor.execute('USE phonepepulseexp')
cursor.execute("SELECT State, Quater, SUM(Transaction_amount) as Total_Transaction_amount FROM Aggregate_transactions1 GROUP BY State, Quater")


filtered_rows = cursor.fetchall()


df = pd.DataFrame(filtered_rows, columns=["State", "Quater", "Transaction_amount"])

conn.commit()


state = st.selectbox("Select State", df["State"].unique())


df = df[df["State"] == state]


fig = px.bar(df, x="Quater", y="Transaction_amount", color="State")


st.plotly_chart(fig)
cursor.execute('USE phonepepulseexp')
cursor.execute("SELECT State, Quater, SUM( User_Brand and Brand_Count) as Brand FROM Aggregate_Users GROUP BY State, Quater")

# Fetch the filtered rows
filtered_rows = cursor.fetchall()

# Create a DataFrame from the filtered rows
df1 = pd.DataFrame(filtered_rows, columns=["State", "Quater", "Brand"])

conn.commit()

# Create a dropdown menu using Streamlit
state = st.selectbox("Select State", df["State"].unique())

# Filter the DataFrame based on the selected state
df = df[df["State"] == state]

# Create a bar chart using Plotly Express
fig = px.bar(df, x="Quater", y="Year", color="State")

# Display the bar chart using Streamlit
st.plotly_chart(fig)
















           
                      
                      
                    
                      
                      
                      






































             
                                      
                   
   