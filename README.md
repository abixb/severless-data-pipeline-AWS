# severless-data-pipeline-AWS
This is the project I'm building for my hands-on Cloud Internship/Project class for Spring 2025. Uses AWS Kinesis, AWS Lambda, AWS DynamoDB+Redshift (for SQL data).
The data ingestion uses a mock temperature sensor built atop a Python script.

# For the iot-data-stream.py

**Generate data from 10 devices every second and print to console**

python iot_data_generator.py

**Generate 100 readings from 5 devices every 2 seconds and save to CSV**

python iot_data_generator.py --devices 5 --frequency 2 --count 100 --output sensor_data.csv --format csv

**Generate continuous data and send to Kinesis (after uncommenting Kinesis code)**

python iot_data_generator.py --kinesis IoTStream


# Summary of the Python script and core features:

**A Python script for generating realistic synthetic IoT sensor data streams. Simulates multiple devices reporting environmental metrics with configurable parameters, anomalies, and output formats. Ideal for testing IoT platforms, analytics pipelines, and cloud integrations.**

## Features

### 📊 Realistic Data Simulation
- **6 Sensor Types**  
  Temperature (°C), Humidity (%), Pressure (hPa), Light (lux), Air Quality (PM2.5), Battery (%)
- **Real-World Patterns**  
  Sensor drift, min/max constraints, battery decay, and 98% reporting reliability
- **Controlled Anomalies**  
  1% chance of sudden value spikes/drops for testing anomaly detection

### 🏭 Operational Realism
- **Device Statuses**  
  Probabilistic status updates (95% operational, 3% maintenance, 1.5% warning, 0.5% error)
- **Location Context**  
  5 preconfigured industrial locations with GPS coordinates + micro-variations
- **Unique Devices**  
  UUID-based device IDs with customizable sensor combinations

### ⚙️ Configuration & Output
- **Multiple Formats**  
  JSON (nested structure) and CSV (flattened via Pandas)
- **Cloud Ready**  
  Prepared scaffold for AWS Kinesis integration (boto3 optional)
- **Command-Line Control**  
  ```bash
  python script.py --devices 50 --frequency 0.5 --count 1000 --output data.csv --format csv




























---
~Abi


