import logging
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import analysis as a


# Streamlit app
st.set_page_config(page_title="Taxi Data Analysis Dashboard", layout="wide")
st.title('Taxi Data Analysis Dashboard')

# Load the data
df_green_taxi = pd.read_csv('scraped_data/green_taxi.csv')

# Convert 'Date' column to datetime format
df_green_taxi['Date'] = pd.to_datetime(df_green_taxi['Date'])

# Extract year and month from 'Date' to aggregate data by month
df_green_taxi['YearMonth'] = df_green_taxi['Date'].dt.to_period('M')

# Group by 'YearMonth' and aggregate total trips
monthly_trips = df_green_taxi.groupby('YearMonth')['total_trip'].sum().reset_index()

# Convert 'YearMonth' back to string for plotting
monthly_trips['YearMonth'] = monthly_trips['YearMonth'].astype(str)

# Plot Green Taxi Data
st.subheader('Green Taxi Total Trips By Month')
fig, ax1 = plt.subplots()

ax1.set_xlabel('Month')
ax1.set_ylabel('Total Trips', color='tab:blue')
ax1.plot(monthly_trips['YearMonth'], monthly_trips['total_trip'], color='tab:blue', marker='o', label='Total Trips')
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.set_xticks(monthly_trips['YearMonth'][::max(1, len(monthly_trips['YearMonth'])//12)])  # Show fewer ticks
ax1.set_xticklabels(monthly_trips['YearMonth'][::max(1, len(monthly_trips['YearMonth'])//12)], rotation=45)

fig.tight_layout()
st.pyplot(fig)

# Load the data
df_yellow_taxi = pd.read_csv('scraped_data/yellow_taxi.csv')

# Convert 'Date' column to datetime format
df_yellow_taxi['Date'] = pd.to_datetime(df_yellow_taxi['Date'])

# Extract year and month from 'Date' to aggregate data by month
df_yellow_taxi['YearMonth'] = df_yellow_taxi['Date'].dt.to_period('M')

# Group by 'YearMonth' and aggregate total trips
monthly_trips = df_yellow_taxi.groupby('YearMonth')['total_trip'].sum().reset_index()

# Convert 'YearMonth' back to string for plotting
monthly_trips['YearMonth'] = monthly_trips['YearMonth'].astype(str)

# Plot Green Taxi Data
st.subheader('Green Taxi Total Trips By Month')
fig, ax1 = plt.subplots()

ax1.set_xlabel('Month')
ax1.set_ylabel('Total Trips', color='tab:blue')
ax1.plot(monthly_trips['YearMonth'], monthly_trips['total_trip'], color='tab:blue', marker='o', label='Total Trips')
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.set_xticks(monthly_trips['YearMonth'][::max(1, len(monthly_trips['YearMonth'])//12)])  # Show fewer ticks
ax1.set_xticklabels(monthly_trips['YearMonth'][::max(1, len(monthly_trips['YearMonth'])//12)], rotation=45)

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

# Load the data
df_fare_analysis_green = pd.read_csv('scraped_data/fare_green_taxi.csv')
df_fare_analysis_yellow = pd.read_csv('scraped_data/fare_Yellow_taxi.csv')

# Prepare data for Green Taxi Heatmap
correlation_matrix_green = df_fare_analysis_green.corr()

# Prepare data for Yellow Taxi Heatmap
correlation_matrix_yellow = df_fare_analysis_yellow.corr()

# Plot Fare Analysis Heatmaps
st.subheader('Correlation Heatmaps for Green and Yellow Taxis')

# Create a figure with 2 subplots side by side
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))  # Adjust the size as needed

# Green Taxi Heatmap
sns.heatmap(correlation_matrix_green,
            annot=True,
            cmap='coolwarm',
            ax=ax1,
            fmt='.2f',
            cbar_kws={'label': 'Correlation Coefficient'})
ax1.set_title('Correlation Heatmap for Green Taxi')
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45)
ax1.set_yticklabels(ax1.get_yticklabels(), rotation=0)

# Yellow Taxi Heatmap
sns.heatmap(correlation_matrix_yellow,
            annot=True,
            cmap='coolwarm',
            ax=ax2,
            fmt='.2f',
            cbar_kws={'label': 'Correlation Coefficient'})
ax2.set_title('Correlation Heatmap for Yellow Taxi')
ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45)
ax2.set_yticklabels(ax2.get_yticklabels(), rotation=0)

# Display the plot
fig.tight_layout()
st.pyplot(fig)