import logging
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import analysis as a


# Streamlit app
st.set_page_config(page_title="Taxi Data Analysis Dashboard", layout="wide")
st.title('Taxi Data Analysis Dashboard')

# Show Data from Green Taxi Aggregated Table
df_green_taxi = pd.read_csv('scraped_data/green_taxi.csv')


# Plot Green Taxi Data
st.subheader('Green Taxi Total Trips By Date')
fig, ax1 = plt.subplots()

ax1.set_xlabel('Date')
ax1.set_ylabel('Total Trips', color='tab:blue')
ax1.plot(df_green_taxi['Date'], df_green_taxi['total_trip'], color='tab:blue', label='Total Trips')
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.set_xticks(df_green_taxi['Date'][::int(len(df_green_taxi['Date'])/20)])  # Show only every 20th date
ax1.set_xticklabels(df_green_taxi['Date'][::int(len(df_green_taxi['Date'])/20)], rotation=45)

fig.tight_layout()
st.pyplot(fig)

# Show Data from Yellow Taxi Aggregated Table
df_yellow_taxi = pd.read_csv('scraped_data/yellow_taxi.csv')

# Plot Yellow Taxi Data
st.subheader('Yellow Taxi Total Trips By Date')
fig, ax1 = plt.subplots()

ax1.set_xlabel('Date')
ax1.set_ylabel('Total Trips', color='tab:blue')
ax1.plot(df_yellow_taxi['Date'], df_yellow_taxi['total_trip'], color='tab:blue', label='Total Trips')
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.set_xticks(df_yellow_taxi['Date'][::int(len(df_yellow_taxi['Date'])/20)])  # Show only every 20th date
ax1.set_xticklabels(df_yellow_taxi['Date'][::int(len(df_yellow_taxi['Date'])/20)], rotation=45)

fig.tight_layout()
st.pyplot(fig)

# Show Peak Time Analysis Data
st.header('Peak Time Analysis')
df_peak_time_green = pd.read_csv('scraped_data/peak_green_taxi.csv')
st.subheader('Peak Time for Green Taxi')

df_peak_time_yellow = pd.read_csv('scraped_data/peak_yellow_taxi.csv')
st.subheader('Peak Time for Yellow Taxi')

# Plot Peak Time Analysis
st.subheader('Peak Time Analysis for Green and Yellow Taxis')
fig, ax = plt.subplots()
sns.lineplot(data=df_peak_time_green, x='Hour', y='total_trip', ax=ax, label='Green Taxi')
sns.lineplot(data=df_peak_time_yellow, x='Hour', y='total_trip', ax=ax, label='Yellow Taxi', color='orange')
ax.set_title('Total Trips by Hour')
ax.set_xlabel('Hour of Day')
ax.set_ylabel('Total Trips')
st.pyplot(fig)

# Show Fare Analysis Data
st.header('Fare Analysis')
df_fare_analysis_green = pd.read_csv('scraped_data/fare_green_taxi.csv')
st.subheader('Fare Analysis for Green Taxi')

df_fare_analysis_yellow = pd.read_csv('scraped_data/fare_Yellow_taxi.csv')
st.subheader('Fare Analysis for Yellow Taxi')

# Plot Fare Analysis
st.subheader('Fare Analysis for Green and Yellow Taxis')
fig, ax = plt.subplots()
sns.scatterplot(data=df_fare_analysis_green, x='passenger_count', y='fare_amount', ax=ax, label='Green Taxi')
sns.scatterplot(data=df_fare_analysis_yellow, x='passenger_count', y='fare_amount', ax=ax, label='Yellow Taxi', color='orange')
ax.set_title('Fare Amount vs. Passenger Count')
ax.set_xlabel('Passenger Count')
ax.set_ylabel('Fare Amount')
st.pyplot(fig)

