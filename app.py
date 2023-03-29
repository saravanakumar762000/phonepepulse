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
from  PIL import Image

mydb = mysql.connect(host="localhost",
                   user="root",
                   password="12345",
                   database= "phonepe_pulse_explore1"
                  )
mycursor = mydb.cursor(buffered=True)
def set_page_config():
    
  st.set_page_config(
      page_title="Phonepe Pulse",
      page_icon="😀"
    
    )

def selection():
   
     selected = st.selectbox("Menu", ["Home","Top_Charts","Explore_Data","About"], 
                           
                            index=0,
                            key="menu",
                            help="Select an option from the menu")
    
     return selected
def Home():
        
        st.markdown("# :violet[Data Visualization and Exploration]")
        st.markdown("## :violet[A User-Friendly Tool Using Streamlit and Plotly]")
        
          
       
def Top_Charts():
        st.markdown("## :violet[Top_Charts]")
        Type = st.selectbox("**Type**", ("Transactions", "Users"))
        colum1,colum2= st.columns([1,1.5],gap="large")
        with colum1:
           Year = st.slider("**Year**", min_value=2018, max_value=2022)
           Quarter = st.slider("Quarter", min_value=1, max_value=4)
    
           
        if Type == "Transactions":
            col1,col2,col3 = st.columns([1,1,1],gap="small")
        
            with col1:
               st.markdown("### :violet[State]")
               mycursor.execute(f"select state, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from agg_transer where year = {Year} and quarter = {Quarter} group by state order by Total desc limit 10")
               df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Transactions_Count','Total_Amount'])
               fig = px.pie(df, values='Total_Amount',
                             names='State',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Transactions_Count'],
                             labels={'Transactions_Count':'Transactions_Count'})

               fig.update_traces(textposition='inside', textinfo='percent+label')
               st.plotly_chart(fig,use_container_width=True)
            
            with col2:
               st.markdown("### :violet[District]")
               mycursor.execute(f"select district , sum(Count) as Total_Count, sum(Amount) as Total from map_trans where year = {Year} and quarter = {Quarter} group by district order by Total desc limit 10")
               df = pd.DataFrame(mycursor.fetchall(), columns=['District', 'Transactions_Count','Total_Amount'])

               fig = px.pie(df, values='Total_Amount',
                             names='District',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Transactions_Count'],
                             labels={'Transactions_Count':'Transactions_Count'})

               fig.update_traces(textposition='inside', textinfo='percent+label')
               st.plotly_chart(fig,use_container_width=True)
            
            with col3:
              st.markdown("### :violet[Pincode]")
              mycursor.execute(f"select Distircts, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from top_trans where year = {Year} and quarter = {Quarter} group by Distircts order by Total desc limit 10")
              df = pd.DataFrame(mycursor.fetchall(), columns=['Pincode', 'Transactions_Count','Total_Amount'])
              fig = px.pie(df, values='Total_Amount',
                             names='Pincode',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Transactions_Count'],
                             labels={'Transactions_Count':'Transactions_Count'})

              fig.update_traces(textposition='inside', textinfo='percent+label')
              st.plotly_chart(fig,use_container_width=True)
            
          
        if Type == "Users":
            col1,col2,col3,col4 = st.columns([2,2,2,2],gap="large")
        
            with col1:
               st.markdown("### :violet[Brands]")
               if Year == 2022 and Quarter in [2,3,4]:
                   st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4")
               else:
                   mycursor.execute(f"select brands, sum(count) as Total_Count, avg(percentage)*100 as Avg_Percentage from agg_users where year = {Year} and quarter = {Quarter} group by brands order by Total_Count desc limit 10")
                   df = pd.DataFrame(mycursor.fetchall(), columns=['Brand', 'Total_Users','Avg_Percentage'])
                   fig = px.bar(df,
                             title='Top 10',
                             x="Total_Users",
                             y="Brand",
                             orientation='h',
                             color='Avg_Percentage',
                             color_continuous_scale=px.colors.sequential.Agsunset)
                   st.plotly_chart(fig,use_container_width=True)   
    
            with col2:
               st.markdown("### :violet[District]")
               mycursor.execute(f"select district, sum(Registered_User) as Total_Users, sum(app_opens) as Total_Appopens from map_user where year = {Year} and quarter = {Quarter} group by district order by Total_Users desc limit 10")
               df = pd.DataFrame(mycursor.fetchall(), columns=['District', 'Total_Users','Total_Appopens'])
               df.Total_Users = df.Total_Users.astype(float)
               fig = px.bar(df,
                         title='Top 10',
                         x="Total_Users",
                         y="District",
                         orientation='h',
                         color='Total_Users',
                         color_continuous_scale=px.colors.sequential.Agsunset)
               st.plotly_chart(fig,use_container_width=True)
              
            with col3:
               st.markdown("### :violet[Pincode]")
               mycursor.execute(f"select Distircts, sum(Registered_Users) as Total_Users from top_user where year = {Year} and quarter = {Quarter} group by Distircts order by Total_Users desc limit 10")
               df = pd.DataFrame(mycursor.fetchall(), columns=['Pincode', 'Total_Users'])
               fig = px.pie(df,
                         values='Total_Users',
                         names='Pincode',
                         title='Top 10',
                         color_discrete_sequence=px.colors.sequential.Agsunset,
                         hover_data=['Total_Users'])
               fig.update_traces(textposition='inside', textinfo='percent+label')
               st.plotly_chart(fig,use_container_width=True)
            
            with col4:
               st.markdown("### :violet[State]")
               mycursor.execute(f"select state, sum(Registered_user) as Total_Users, sum(App_opens) as Total_Appopens from map_user where year = {Year} and quarter = {Quarter} group by state order by Total_Users desc limit 10")
               df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
               fig = px.pie(df, values='Total_Users',
                             names='State',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Appopens'],
                             labels={'Total_Appopens':'Total_Appopens'})

               fig.update_traces(textposition='inside', textinfo='percent+label')
               st.plotly_chart(fig,use_container_width=True)
            
       
def Explore_Data():
        Year = st.sidebar.slider("**Year**", min_value=2018, max_value=2022)
        Quarter = st.sidebar.slider("Quarter", min_value=1, max_value=4)
        Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
        col1,col2 = st.columns(2)
    

        if Type == "Transactions":
        
         
            with col1:
                st.markdown("## :violet[Overall State Data - Transactions Amount]")
                mycursor.execute(f"select state, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_trans where year = {Year} and quarter = {Quarter} group by state order by state")
                df1 = pd.DataFrame(mycursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
                df2 = pd.read_csv('statenames.csv')
                df1.State = df2

                fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                      featureidkey='properties.ST_NM',
                      locations='State',
                      color='Total_amount',
                      color_continuous_scale='sunset')

                fig.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig,use_container_width=True)
            
        
            with col2:
            
                st.markdown("## :violet[Overall State Data - Transactions Count]")
                mycursor.execute(f"select state, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_trans where year = {Year} and quarter = {Quarter} group by state order by state")
                df1 = pd.DataFrame(mycursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
                df2 = pd.read_csv('Statenames.csv')
                df1.Total_Transactions = df1.Total_Transactions.astype(int)
                df1.State = df2

                fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                      featureidkey='properties.ST_NM',
                      locations='State',
                      color='Total_Transactions',
                      color_continuous_scale='sunset')

                fig.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig,use_container_width=True)
            
            
            

                st.markdown("## :violet[Top Payment Type]")
                mycursor.execute(f"select Transaction_type, sum(Transaction_count) as Total_Transactions, sum(Transaction_amount) as Total_amount from agg_transer where year= {Year} and quarter = {Quarter} group by transaction_type order by Transaction_type")
                df = pd.DataFrame(mycursor.fetchall(), columns=['Transaction_type', 'Total_Transactions','Total_amount'])

                fig = px.bar(df,
                     title='Transaction Types vs Total_Transactions',
                     x="Transaction_type",
                     y="Total_Transactions",
                     orientation='v',
                     color='Total_amount',
                     color_continuous_scale=px.colors.sequential.Agsunset)
                st.plotly_chart(fig,use_container_width=False)
        
          
                st.markdown("# ")
                st.markdown("# ")
                st.markdown("# ")
                st.markdown("## :violet[Select any State to explore more]")
                selected_state = st.selectbox("",
                             ('andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar',
                              'chandigarh','chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu','delhi','goa','gujarat','haryana',
                              'himachal-pradesh','jammu-&-kashmir','jharkhand','karnataka','kerala','ladakh','lakshadweep',
                              'madhya-pradesh','maharashtra','manipur','meghalaya','mizoram',
                              'nagaland','odisha','puducherry','punjab','rajasthan','sikkim',
                              'tamil-nadu','telangana','tripura','uttar-pradesh','uttarakhand','west-bengal'),index=30)
         
                mycursor.execute(f"select State, District,year,quarter, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_trans where year = {Year} and quarter = {Quarter} and State = '{selected_state}' group by State, District,year,quarter order by state,district")
        
                df1 = pd.DataFrame(mycursor.fetchall(), columns=['State','District','Year','Quarter',
                                                         'Total_Transactions','Total_amount'])
                fig = px.bar(df1,
                     title=selected_state,
                     x="District",
                     y="Total_Transactions",
                     orientation='v',
                     color='Total_amount',
                     color_continuous_scale=px.colors.sequential.Agsunset)
                st.plotly_chart(fig,use_container_width=True)
        
   
        if Type == "Users":
        
        
            st.markdown("## :violet[Overall State Data - User App opening frequency]")
            mycursor.execute(f"select state, sum(Registered_user) as Total_Users, sum(App_opens) as Total_Appopens from map_user where year = {Year} and quarter = {Quarter} group by state order by state")
            df1 = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
            df2 = pd.read_csv('Statenames.csv')
            df1.Total_Appopens = df1.Total_Appopens.astype(float)
            df1.State = df2
        
            fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                  featureidkey='properties.ST_NM',
                  locations='State',
                  color='Total_Appopens',
                  color_continuous_scale='sunset')

            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig,use_container_width=True)
        
        
            st.markdown("## :violet[Select any State to explore more]")
            selected_state = st.selectbox("",
                             ('andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar',
                              'chandigarh','chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu','delhi','goa','gujarat','haryana',
                              'himachal-pradesh','jammu-&-kashmir','jharkhand','karnataka','kerala','ladakh','lakshadweep',
                              'madhya-pradesh','maharashtra','manipur','meghalaya','mizoram',
                              'nagaland','odisha','puducherry','punjab','rajasthan','sikkim',
                              'tamil-nadu','telangana','tripura','uttar-pradesh','uttarakhand','west-bengal'),index=30)
        
            mycursor.execute(f"select State,year,quarter,District,sum(Registered_user) as Total_Users, sum(App_opens) as Total_Appopens from map_user where year = {Year} and quarter = {Quarter} and state = '{selected_state}' group by State, District,year,quarter order by state,district")
        
            df = pd.DataFrame(mycursor.fetchall(), columns=['State','year', 'quarter', 'District', 'Total_Users','Total_Appopens'])
            df.Total_Users = df.Total_Users.astype(int)
        
            fig = px.bar(df,
                     title=selected_state,
                     x="District",
                     y="Total_Users",
                     orientation='v',
                     color='Total_Users',
                     color_continuous_scale=px.colors.sequential.Agsunset)
            st.plotly_chart(fig,use_container_width=True)

     
if __name__ == '__main__':
    set_page_config()
    
    selected=selection()
    if selected == "Home":
        Home()
    elif selected == "Top_Charts":
        Top_Charts()

    elif selected == "Explore_Data":
        Explore_Data()
     
        
        
        
        
