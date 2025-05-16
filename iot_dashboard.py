import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
from decimal import Decimal
from datetime import datetime

# --- Page Configuration (Optional but nice) ---
st.set_page_config(layout="wide", page_title="IoT Sensor Dashboard")

# --- AWS Configuration ---
# Ensure your AWS credentials are configured (e.g., via `aws configure` or environment variables)
# Boto3 will pick them up automatically.
AWS_REGION = "us-east-1"  # <--- CHANGE THIS TO YOUR DYANMODB TABLE'S REGION
DYNAMODB_TABLE_NAME = "IoTData" # <--- CHANGE THIS IF YOUR TABLE NAME IS DIFFERENT

# --- Helper Function to Convert DynamoDB Decimals and other types ---
def convert_dynamodb_item(item):
    """ Recursively converts Decimal objects in a DynamoDB item to floats/ints,
        and handles other potential conversions for plotting.
    """
    if isinstance(item, list):
        return [convert_dynamodb_item(i) for i in item]
    elif isinstance(item, dict):
        new_dict = {}
        for k, v in item.items():
            if isinstance(v, Decimal):
                if v % 1 == 0: # Check if it's a whole number
                    new_dict[k] = int(v)
                else:
                    new_dict[k] = float(v)
            elif k == 'timestamp' and isinstance(v, str): # Convert timestamp string to datetime
                try:
                    new_dict[k] = datetime.fromisoformat(v)
                except ValueError:
                    new_dict[k] = v # Keep original if parsing fails
            else:
                new_dict[k] = convert_dynamodb_item(v) # Recurse for nested structures
        return new_dict
    else:
        return item

# --- Function to Fetch Data from DynamoDB ---
# Using st.cache_data to cache results and avoid re-fetching on every interaction
@st.cache_data(ttl=60) # Cache for 60 seconds
def fetch_data_from_dynamodb(limit=500): # Fetch more data for better visuals
    """ Fetches data from DynamoDB and converts items for Pandas/Plotly.
        For a demo, scanning is okay. For production with large tables,
        you'd use targeted queries or export data to S3/Athena.
    """
    try:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        
        # Scan operation can be expensive on large tables.
        # Consider using query if you have specific access patterns.
        # For this demo, we'll scan a limited number of items.
        response = table.scan(Limit=limit)
        items = response.get('Items', [])
        
        # Handle pagination if you want to fetch more than the initial scan limit
        while 'LastEvaluatedKey' in response and len(items) < limit:
            response = table.scan(Limit=limit - len(items), ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
            if len(items) >= limit: # Respect the overall limit
                break
        
        if not items:
            return pd.DataFrame()
            
        # Convert DynamoDB items (especially Decimals)
        converted_items = [convert_dynamodb_item(item) for item in items]
        
        df = pd.DataFrame(converted_items)
        
        # Ensure essential columns exist
        if 'timestamp' not in df.columns:
            st.error("Timestamp column is missing from the fetched data!")
            return pd.DataFrame()
        if 'device_id' not in df.columns: # Add device_id if it's not there but needed
            df['device_id'] = 'unknown_device' 
            
        return df
    except Exception as e:
        st.error(f"Error fetching data from DynamoDB: {e}")
        return pd.DataFrame()

# --- Main Application ---
st.title("Real-Time IoT Sensor Data Dashboard")
st.markdown("Visualizing data streamed from IoT devices into AWS DynamoDB.")

# --- Sidebar for Controls (Optional) ---
st.sidebar.header("Dashboard Controls")
data_limit = st.sidebar.slider("Number of recent records to fetch:", 100, 2000, 500, step=100)
# auto_refresh = st.sidebar.checkbox("Auto-refresh data (every 60s)", value=False) # More advanced

# --- Fetch Data ---
# Add a button to manually refresh data
if st.sidebar.button("Refresh Data"):
    # Clear the cache for fetch_data_from_dynamodb to force a new fetch
    st.cache_data.clear() 
    
df_iot = fetch_data_from_dynamodb(limit=data_limit)

if df_iot.empty:
    st.warning("No data fetched from DynamoDB, or an error occurred. Is your data generator script running and writing to DynamoDB?")
    st.stop() # Stop execution if no data

# --- Data Preprocessing for Visualizations ---
# Ensure timestamp is datetime and sort
df_iot['timestamp'] = pd.to_datetime(df_iot['timestamp'])
df_iot = df_iot.sort_values(by='timestamp')

# Extract individual sensor readings into their own columns for easier plotting
# This requires knowing your 'readings' structure.
# Example: if readings = {'temperature': {'value': 20, 'unit': 'C'}, 'humidity': {'value': 60, 'unit': '%'}}
def extract_sensor_value(reading_dict, sensor_name):
    if isinstance(reading_dict, dict) and \
       isinstance(reading_dict.get(sensor_name), dict):
        return reading_dict[sensor_name].get('value')
    return None

# Create columns for each sensor you want to plot
# Adjust 'temperature', 'humidity', etc. to match your actual sensor keys in the 'readings' dict
df_iot['temperature'] = df_iot['readings'].apply(lambda x: extract_sensor_value(x, 'temperature'))
df_iot['humidity'] = df_iot['readings'].apply(lambda x: extract_sensor_value(x, 'humidity'))
df_iot['pressure'] = df_iot['readings'].apply(lambda x: extract_sensor_value(x, 'pressure'))
df_iot['light_level'] = df_iot['readings'].apply(lambda x: extract_sensor_value(x, 'light_level'))
df_iot['air_quality'] = df_iot['readings'].apply(lambda x: extract_sensor_value(x, 'air_quality'))
df_iot['battery_level'] = df_iot['readings'].apply(lambda x: extract_sensor_value(x, 'battery_level'))


# --- Display Data and Visualizations ---
st.subheader(f"Displaying Last {len(df_iot)} Sensor Readings")

# Optional: Display a sample of the raw (processed) data
if st.checkbox("Show Raw Data Sample"):
    st.dataframe(df_iot.head())

# --- Time Series Plots ---
st.header("Sensor Readings Over Time")

# Device ID selector
device_list = ['All Devices'] + sorted(df_iot['device_id'].unique().tolist())
selected_device = st.selectbox("Select Device ID to filter plots:", device_list)

if selected_device == 'All Devices':
    df_plot = df_iot
else:
    df_plot = df_iot[df_iot['device_id'] == selected_device]


if not df_plot.empty:
    col1, col2 = st.columns(2)
    with col1:
        if 'temperature' in df_plot.columns and not df_plot['temperature'].isnull().all():
            fig_temp = px.line(df_plot.dropna(subset=['temperature']), 
                               x='timestamp', y='temperature', color='device_id',
                               title=f"Temperature (°C) - {selected_device}",
                               labels={'temperature': 'Temperature (°C)'})
            st.plotly_chart(fig_temp, use_container_width=True)
        else:
            st.write(f"No temperature data to display for {selected_device}.")

        if 'pressure' in df_plot.columns and not df_plot['pressure'].isnull().all():
            fig_pressure = px.line(df_plot.dropna(subset=['pressure']),
                                   x='timestamp', y='pressure', color='device_id',
                                   title=f"Pressure (hPa) - {selected_device}",
                                   labels={'pressure': 'Pressure (hPa)'})
            st.plotly_chart(fig_pressure, use_container_width=True)
        else:
            st.write(f"No pressure data to display for {selected_device}.")

    with col2:
        if 'humidity' in df_plot.columns and not df_plot['humidity'].isnull().all():
            fig_humidity = px.line(df_plot.dropna(subset=['humidity']),
                                   x='timestamp', y='humidity', color='device_id',
                                   title=f"Humidity (%) - {selected_device}",
                                   labels={'humidity': 'Humidity (%)'})
            st.plotly_chart(fig_humidity, use_container_width=True)
        else:
            st.write(f"No humidity data to display for {selected_device}.")
            
        if 'battery_level' in df_plot.columns and not df_plot['battery_level'].isnull().all():
            fig_battery = px.line(df_plot.dropna(subset=['battery_level']),
                                   x='timestamp', y='battery_level', color='device_id',
                                   title=f"Battery Level (%) - {selected_device}",
                                   labels={'battery_level': 'Battery Level (%)'})
            st.plotly_chart(fig_battery, use_container_width=True)
        else:
            st.write(f"No battery level data to display for {selected_device}.")

    # --- Other Visualizations ---
    st.header("Device Status Distribution")
    if 'status' in df_plot.columns and not df_plot['status'].isnull().all():
        status_counts = df_plot['status'].value_counts().reset_index()
        status_counts.columns = ['status', 'count']
        fig_status = px.bar(status_counts, x='status', y='count', color='status',
                            title=f"Device Status Counts - {selected_device}")
        st.plotly_chart(fig_status, use_container_width=True)
    else:
        st.write(f"No status data to display for {selected_device}.")

else:
    st.warning(f"No data available for device: {selected_device} with current filters.")


# --- Auto-refresh (more advanced, requires careful implementation) ---
# if auto_refresh:
#    time.sleep(60)
#    st.experimental_rerun()