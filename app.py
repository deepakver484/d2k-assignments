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

st.write(
    "## Insights on Taxi Booking Trends from February to December\n"
    "The data reveals a notable downward trend in taxi bookings from February through December. Specifically, February stands out as the month with the highest number of taxi bookings, marking it as the peak period of the year for the service. \n\n"
    "Following February, there is a consistent decrease in bookings each month, culminating in December with the lowest booking figures of the year. This decline suggests a potential seasonal variation in demand for taxi services, which could be influenced by factors such as weather conditions, holiday schedules, or changes in consumer behavior over the year.\n\n"
    "Understanding this trend is crucial for optimizing taxi service operations and marketing strategies throughout the year."
)
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

st.write(
    "## Insights on Green Taxi Booking Trends from February to December\n"
    "The analysis of green taxi booking data reveals a distinct trend over the course of the year. February emerges as the month with the highest number of taxi bookings, establishing it as the peak period for green taxi services. \n\n"
    "Following this peak in February, there is a noticeable downward trend in bookings that continues for eight consecutive months, reaching its lowest point in October. This decline suggests a period of reduced demand for green taxi services.\n\n"
    "However, from October onwards, there is a significant reversal of this trend, with bookings starting to increase again. This upward trend toward the end of the year indicates a resurgence in demand for green taxis, which could be attributed to factors such as changes in consumer behavior, promotional activities, or seasonal events.\n\n"
    "Understanding these trends is essential for effectively managing green taxi service operations and tailoring marketing strategies throughout the year."
)

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

st.write(
    "## Insights on Peak and Lowest Taxi Booking Hours for Green and Traditional Taxis\n"
    "The analysis of taxi booking data for both green and traditional taxis reveals distinct patterns in booking hours throughout the day.\n\n"
    "### Peak Booking Hours\n"
    "For both green and traditional taxis, the peak booking hours are observed between **3:00 PM and 8:00 PM**. This period shows the highest demand for taxi services, indicating that customers are most active during these hours. Factors contributing to this peak could include the end of the workday, increased social activities, or the need for transportation during evening hours.\n\n"
    "### Lowest Booking Hours\n"
    "Conversely, the lowest booking hours for both types of taxis are between **12:00 AM and 5:00 AM**. This period exhibits the least amount of taxi bookings, which may be attributed to reduced transportation needs during late night and early morning hours.\n\n"
    "Understanding these peak and low-demand hours is crucial for optimizing taxi service operations, adjusting staffing levels, and planning targeted marketing efforts to improve service availability and meet customer needs effectively."
)

# Show Fare Analysis Data
st.header('Fare Analysis')

# Load the data
df_fare_analysis_green = pd.read_csv('scraped_data/fare_green_taxi.csv')
df_fare_analysis_yellow = pd.read_csv('scraped_data/fare_yellow_taxi.csv')

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

st.write(
    "## Analysis of Heat Map: Passenger Count vs. Fare Amount for Yellow and Green Taxis\n"
    "The heat map analysis provides valuable insights into the relationship between passenger count and fare amount for both yellow and green taxis. \n\n"
    "### Correlation Analysis\n"
    "From the heat map, we observe a negative correlation between passenger count and fare amount for both types of taxis. Specifically, the correlation coefficients are as follows:\n"
    "- **Yellow Taxi:** The correlation coefficient is **-0.28**, indicating a weak negative correlation between the number of passengers and the fare amount. This suggests that, generally, as the number of passengers increases, the fare amount tends to decrease slightly, although the relationship is not very strong.\n"
    "- **Green Taxi:** The correlation coefficient is **-0.31**, showing a moderate negative correlation between the number of passengers and the fare amount. This implies a more noticeable trend where, as the number of passengers increases, the fare amount tends to decrease more consistently compared to yellow taxis.\n\n"
    "### Interpretation\n"
    "These negative correlations suggest that for both yellow and green taxis, higher passenger counts are associated with lower fare amounts. This could be due to the fact that more passengers often mean the fare is split among several individuals, or that longer trips with more passengers might result in discounts or lower per-passenger fares.\n\n"
    "Understanding this relationship helps in analyzing fare structures and can inform strategies for pricing, promotions, and service adjustments to better align with passenger needs and business objectives."
)
