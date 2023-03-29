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

mydb = mysql.connect(host="localhost",
                   user="root",
                   password='12345',
                   database= "phonepe_pulse_explore1"
                  )
mycursor = mydb.cursor(buffered=True)

mycursor.execute("CREATE DATABASE phonepe_pulse_explore1")
mydb = mysql.connect(host="localhost",
                   user="root",
                   password='12345',
                   database= "phonepe_pulse_explore1"
                  )
mycursor = mydb.cursor(buffered=True)

mycursor.execute("create table agg_transer (State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount double)")

for i,row in df.iterrows():
    sql = "INSERT INTO agg_transer VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()
    
mycursor.execute("create table agg_users (State varchar(100), Year int, Quarter int, Brands varchar(100), Count int, Percentage double)")

for i,row in df2.iterrows():
    sql = "INSERT INTO agg_users VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table map_trans (State varchar(100), Year int, Quarter int, District varchar(100), Count int, Amount double)")

for i,row in df3.iterrows():
    sql = "INSERT INTO map_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()


mycursor.execute("create table map_user (State varchar(100), Year int, Quarter int, District varchar(100), Registered_user int, App_opens int)")

for i,row in df4.iterrows():
    sql = "INSERT INTO map_user VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()


mycursor.execute("create table top_trans (State varchar(100), Year int, Quarter int,  Distircts varchar(100), Transaction_count int, Transaction_amount double)")

for i,row in df5.iterrows():
    sql = "INSERT INTO top_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table top_user (State varchar(100), Year int, Quarter int, Distircts varchar(100) , Registered_users int)")

for i,row in df6.iterrows():
    sql = "INSERT INTO top_user VALUES (%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()
    
mycursor.execute("show tables")
result=mycursor.fetchall()
for i in result:
    print(i)
    
    





                                      
                   
   
