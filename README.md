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


~Abi
