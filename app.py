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

df_peak_time_yellow = pd.read_csv('scraped_data/peak_yellow_taxi.csv')

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

df_fare_analysis_yellow = pd.read_csv('scraped_data/fare_Yellow_taxi.csv')

# Prepare data for Green Taxi Heatmap
pivot_table_green = df_fare_analysis_green.pivot_table(
    index='passenger_count',
    columns='taxi_type',
    values='fare_amount',
    aggfunc='mean',  
    fill_value=0  # Fill missing values with 0
)
pivot_table_green['taxi_type'] = 'Green Taxi'

# Prepare data for Yellow Taxi Heatmap
pivot_table_yellow = df_fare_analysis_yellow.pivot_table(
    index='passenger_count',
    columns='taxi_type',
    values='fare_amount',
    aggfunc='mean',  
    fill_value=0  # Fill missing values with 0
)
pivot_table_yellow['taxi_type'] = 'Yellow Taxi'

# Plot Fare Analysis Heatmap for Green Taxi
st.subheader('Fare Analysis Heatmap for Green Taxi')
fig, ax = plt.subplots(figsize=(10, 6))  # Adjust the size as needed
sns.heatmap(pivot_table_green.pivot('passenger_count', 'taxi_type', 'fare_amount'),
            annot=True,
            cmap='coolwarm',
            ax=ax,
            fmt='.2f',
            cbar_kws={'label': 'Average Fare Amount'})
ax.set_title('Heatmap of Average Fare Amount for Green Taxi')
ax.set_xlabel('Taxi Type')
ax.set_ylabel('Passenger Count')
st.pyplot(fig)

# Plot Fare Analysis Heatmap for Yellow Taxi
st.subheader('Fare Analysis Heatmap for Yellow Taxi')
fig, ax = plt.subplots(figsize=(10, 6))  # Adjust the size as needed
sns.heatmap(pivot_table_yellow.pivot('passenger_count', 'taxi_type', 'fare_amount'),
            annot=True,
            cmap='coolwarm',
            ax=ax,
            fmt='.2f',
            cbar_kws={'label': 'Average Fare Amount'})
ax.set_title('Heatmap of Average Fare Amount for Yellow Taxi')
ax.set_xlabel('Taxi Type')
ax.set_ylabel('Passenger Count')
st.pyplot(fig)
