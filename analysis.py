# import streamlit as st
import pandas as pd
import sqlite3  # Replace with your specific database library if not using SQLite
# import matplotlib.pyplot as plt
# import seaborn as sns

# Function to connect to the database and fetch data
def fetch_data(query):
    conn = sqlite3.connect('testdb.db')  # Replace with your database connection string
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(df)
    return df

# Fetch data
# df_green_taxi = fetch_data(query_green_taxi)
# df_yellow_taxi = fetch_data()

# Streamlit app
# st.title('Taxi Data Analysis')

# # Green Taxi Data
# st.header('Green Taxi Data')
# st.write(df_green_taxi)

# # Plot Green Taxi Data
# st.subheader('Green Taxi Total Trips Over Time')
# fig, ax = plt.subplots()
# sns.lineplot(data=df_green_taxi, x='lpep_pickup_datetime', y='total_trip', ax=ax)
# ax.set_title('Total Trips for Green Taxi')
# ax.set_xlabel('Pickup Datetime')
# ax.set_ylabel('Total Trips')
# st.pyplot(fig)

# st.subheader('Green Taxi Average Fare Over Time')
# fig, ax = plt.subplots()
# sns.lineplot(data=df_green_taxi, x='lpep_pickup_datetime', y='avg_fare_amount', ax=ax)
# ax.set_title('Average Fare Amount for Green Taxi')
# ax.set_xlabel('Pickup Datetime')
# ax.set_ylabel('Average Fare Amount')
# st.pyplot(fig)

# # Yellow Taxi Data
# st.header('Yellow Taxi Data')
# st.write(df_yellow_taxi)

# # Plot Yellow Taxi Data
# st.subheader('Yellow Taxi Total Trips Over Time')
# fig, ax = plt.subplots()
# sns.lineplot(data=df_yellow_taxi, x='tpep_pickup_datetime', y='total_trip', ax=ax)
# ax.set_title('Total Trips for Yellow Taxi')
# ax.set_xlabel('Pickup Datetime')
# ax.set_ylabel('Total Trips')
# st.pyplot(fig)

# st.subheader('Yellow Taxi Average Fare Over Time')
# fig, ax = plt.subplots()
# sns.lineplot(data=df_yellow_taxi, x='tpep_pickup_datetime', y='avg_fare_amount', ax=ax)
# ax.set_title('Average Fare Amount for Yellow Taxi')
# ax.set_xlabel('Pickup Datetime')
# ax.set_ylabel('Average Fare Amount')
# st.pyplot(fig)

