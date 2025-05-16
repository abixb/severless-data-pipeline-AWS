# Author: Balaji "Abi" Abishek
# Description: This is a python script that generates mock data for AWS Kinesis stream, processed through AWS Lambda serverless function



#!/usr/bin/env python3
"""
IoT Sensor Data Generator

This script generates synthetic IoT sensor data to simulate multiple sensors reporting
environmental measurements in real-time -- for the Kinesis data stream. The data is formatted as JSON and can be
configured to stream to AWS Kinesis - which will be linked later.

Generated data includes:
- Temperature (°C)
- Humidity (%)
- Pressure (hPa)
- Light level (lux)
- Air quality (PM2.5)
- Battery level (%)
- GPS coordinates (latitude, longitude)

Each record includes:
- device_id: Unique identifier for the IoT device
- timestamp: ISO format timestamp
- location_id: Identifier for the device location
- readings: Sensor readings with appropriate units
- status: Device operational status
"""

import json
import time
import random
import uuid
import datetime
import argparse
import logging
from typing import Dict, List, Any

import pandas as pd
import numpy as np

# Optional: boto3 for AWS Kinesis integration (commented out until needed)
import boto3

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('iot_data_generator')

# Define sensor types and their characteristics
SENSOR_TYPES = {
    'temperature': {
        'unit': '°C',
        'min': -10,
        'max': 45,
        'precision': 1,
        'drift_factor': 0.1,  # How much the values can drift between readings
    },
    'humidity': {
        'unit': '%',
        'min': 0,
        'max': 100,
        'precision': 1,
        'drift_factor': 2,
    },
    'pressure': {
        'unit': 'hPa',
        'min': 970,
        'max': 1050,
        'precision': 1,
        'drift_factor': 0.5,
    },
    'light_level': {
        'unit': 'lux',
        'min': 0,
        'max': 10000,
        'precision': 0,
        'drift_factor': 50,
    },
    'air_quality': {
        'unit': 'PM2.5',
        'min': 0,
        'max': 500,
        'precision': 1,
        'drift_factor': 5,
    },
    'battery_level': {
        'unit': '%',
        'min': 0,
        'max': 100,
        'precision': 0,
        'drift_factor': 0.1,
        'decay': True,  # Battery levels generally decrease over time
    },
}

# Define locations for the sensors with realistic names and GPS coordinates -- I chose Seattle area coordinates
LOCATIONS = [
    {'id': 'warehouse_a', 'name': 'Warehouse A', 'lat': 47.6062, 'lon': -122.3321},
    {'id': 'warehouse_b', 'name': 'Warehouse B', 'lat': 47.6152, 'lon': -122.3447},
    {'id': 'office_main', 'name': 'Main Office', 'lat': 47.6205, 'lon': -122.3493},
    {'id': 'production_floor', 'name': 'Production Floor', 'lat': 47.6170, 'lon': -122.3377},
    {'id': 'storage_cold', 'name': 'Cold Storage', 'lat': 47.6180, 'lon': -122.3399},
]

# Device status options
DEVICE_STATUSES = ['operational', 'maintenance', 'warning', 'error']
STATUS_WEIGHTS = [0.95, 0.03, 0.015, 0.005]  # Probabilities for each status

class SensorDevice:
    """Represents an IoT sensor device with consistent data generation patterns"""
    
    def __init__(self, device_id: str, location: Dict[str, Any], sensor_types: List[str]):
        """
        Initialize a sensor device with specified parameters
        
        Args:
            device_id: Unique identifier for the device
            location: Dictionary containing location information
            sensor_types: List of sensor types this device will report
        """
        self.device_id = device_id
        self.location = location
        self.sensor_types = sensor_types
        self.current_values = {}
        
        # Initialize each sensor with a random value within its range
        for sensor_type in sensor_types:
            if sensor_type in SENSOR_TYPES:
                sensor_config = SENSOR_TYPES[sensor_type]
                self.current_values[sensor_type] = random.uniform(
                    sensor_config['min'], 
                    sensor_config['max']
                )
    
    def generate_reading(self) -> Dict[str, Any]:
        """
        Generate a sensor reading with realistic data patterns
        
        Returns:
            Dictionary containing the sensor reading data
        """
        timestamp = datetime.datetime.now().isoformat()
        readings = {}
        
        # Generate new values for each sensor with realistic drift from previous values
        for sensor_type in self.sensor_types:
            if sensor_type in SENSOR_TYPES:
                sensor_config = SENSOR_TYPES[sensor_type]
                
                # Calculate drift based on the sensor's drift factor
                drift = random.uniform(
                    -sensor_config['drift_factor'], 
                    sensor_config['drift_factor']
                )
                
                # Apply drift and handle decay for battery level
                if sensor_config.get('decay', False):
                    # Battery levels should slowly decrease over time
                    drift = abs(drift) * -1
                
                # Update the current value with drift
                new_value = self.current_values[sensor_type] + drift
                
                # Ensure the new value stays within the defined range
                new_value = max(sensor_config['min'], min(new_value, sensor_config['max']))
                
                # Round to the specified precision
                new_value = round(new_value, sensor_config['precision'])
                self.current_values[sensor_type] = new_value
                
                # Add the reading with its unit
                readings[sensor_type] = {
                    'value': new_value,
                    'unit': sensor_config['unit']
                }
        
        # Randomly select a status based on the defined weights
        status = random.choices(DEVICE_STATUSES, weights=STATUS_WEIGHTS)[0]
        
        # Introduce occasional anomalies for realism
        if random.random() < 0.01:  # 1% chance of anomaly
            anomaly_sensor = random.choice(self.sensor_types)
            if anomaly_sensor in readings:
                if random.random() < 0.5:
                    # Sudden spike
                    readings[anomaly_sensor]['value'] = SENSOR_TYPES[anomaly_sensor]['max']
                else:
                    # Sudden drop
                    readings[anomaly_sensor]['value'] = SENSOR_TYPES[anomaly_sensor]['min']
        
        # Construct the full reading data
        reading_data = {
            'device_id': self.device_id,
            'timestamp': timestamp,
            'location_id': self.location['id'],
            'location_name': self.location['name'],
            'coordinates': {
                'latitude': self.location['lat'] + random.uniform(-0.0001, 0.0001),  # Tiny random variation
                'longitude': self.location['lon'] + random.uniform(-0.0001, 0.0001)
            },
            'readings': readings,
            'status': status
        }
        
        return reading_data


class DataGenerator:
    """Manages the generation of IoT sensor data from multiple devices"""
    
    def __init__(self, num_devices: int = 10):
        """
        Initialize the data generator with the specified number of devices
        
        Args:
            num_devices: Number of IoT devices to simulate
        """
        self.devices = []
        
        # Create devices with random configurations
        for i in range(num_devices):
            # Generate a unique device ID
            device_id = f"device_{uuid.uuid4().hex[:8]}"
            
            # Randomly select a location for this device
            location = random.choice(LOCATIONS)
            
            # Decide which sensor types this device will have
            # Some devices will have all sensors, others will have a subset
            available_sensors = list(SENSOR_TYPES.keys())
            num_sensors = random.randint(max(1, len(available_sensors) - 2), len(available_sensors))
            device_sensors = random.sample(available_sensors, num_sensors)
            
            # Create the device
            device = SensorDevice(device_id, location, device_sensors)
            self.devices.append(device)
        
        logger.info(f"Initialized {num_devices} IoT sensor devices")
    
    def generate_batch(self) -> List[Dict[str, Any]]:
        """
        Generate a batch of readings from all devices
        
        Returns:
            List of dictionaries containing readings from all devices
        """
        readings = []
        for device in self.devices:
            # Each device has a small chance of not reporting in any given cycle
            if random.random() < 0.98:  # 98% chance of reporting
                readings.append(device.generate_reading())
        
        return readings
    
    def send_to_kinesis(self, stream_name: str, batch: List[Dict[str, Any]]) -> None:
        """
        Send a batch of readings to AWS Kinesis
        
        Args:
            stream_name: Name of the Kinesis stream
            batch: List of readings to send
        """
        # This is a placeholder for AWS Kinesis integration
        # Uncomment and modify when ready to connect to AWS
        
        # --- Start Paste Here ---
        kinesis_client = boto3.client(
            'kinesis',
            region_name='us-east-1'  # <--- !!! IMPORTANT: CHANGE TO YOUR AWS REGION !!!
        )

        records_to_send = []
        for reading in batch:
            records_to_send.append({
                    'Data': json.dumps(reading),
                    'PartitionKey': reading['device_id']
                })

        if records_to_send:
            try:
                response = kinesis_client.put_records(
                    Records=records_to_send,
                    StreamName=stream_name
                )
                logger.info(f"Sent {len(records_to_send)} records to Kinesis stream '{stream_name}'. Failed records: {response.get('FailedRecordCount', 0)}")
                # Optional: Log details if some records failed (helpful for debugging)
                if response.get('FailedRecordCount', 0) > 0:
                    logger.warning(f"Failed records details: {response.get('Records', [])}")

            except Exception as e:
                logger.error(f"Failed to send batch to Kinesis: {e}")
        # --- End Paste Here ---


def save_to_file(data: List[Dict[str, Any]], filename: str) -> None:
    """
    Save generated data to a JSON file
    
    Args:
        data: List of sensor readings
        filename: Name of the file to save to
    """
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved {len(data)} records to {filename}")


def save_to_csv(data: List[Dict[str, Any]], filename: str) -> None:
    """
    Save generated data to a CSV file
    
    This flattens the nested JSON structure for easier analysis in tools like Excel
    
    Args:
        data: List of sensor readings
        filename: Name of the CSV file to save to
    """
    # Flatten the nested JSON structure
    flattened_data = []
    
    for record in data:
        flat_record = {
            'device_id': record['device_id'],
            'timestamp': record['timestamp'],
            'location_id': record['location_id'],
            'location_name': record['location_name'],
            'latitude': record['coordinates']['latitude'],
            'longitude': record['coordinates']['longitude'],
            'status': record['status']
        }
        
        # Add each sensor reading to the flattened record
        for sensor_type, reading in record['readings'].items():
            flat_record[f"{sensor_type}_value"] = reading['value']
            flat_record[f"{sensor_type}_unit"] = reading['unit']
        
        flattened_data.append(flat_record)
    
    # Convert to DataFrame and save to CSV
    df = pd.DataFrame(flattened_data)
    df.to_csv(filename, index=False)
    logger.info(f"Saved {len(flattened_data)} records to {filename}")


def main():
    """Main function to run the IoT data generator"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate synthetic IoT sensor data')
    parser.add_argument('--devices', type=int, default=10, help='Number of IoT devices to simulate')
    parser.add_argument('--frequency', type=float, default=1.0, help='Frequency of data generation in seconds')
    parser.add_argument('--count', type=int, default=0, help='Number of batches to generate (0 for infinite)')
    parser.add_argument('--output', type=str, default='', help='Filename to save data (leave empty for no file output)')
    parser.add_argument('--format', type=str, choices=['json', 'csv'], default='json', help='Output file format')
    parser.add_argument('--kinesis', type=str, default='', help='AWS Kinesis stream name (leave empty for no Kinesis output)')
    
    args = parser.parse_args()
    
    # Initialize the data generator
    generator = DataGenerator(num_devices=args.devices)
    
    # Initialize variables for storing data if saving to file
    all_data = []
    
    # Main loop for data generation
    count = 0
    try:
        while args.count == 0 or count < args.count:
            # Generate a batch of data
            batch = generator.generate_batch()
            
            # Print a sample reading to console
            if batch:
                logger.info(f"Generated {len(batch)} readings")
                logger.info(f"Sample: {json.dumps(batch[0], indent=2)}")
            
            # Send to Kinesis if specified
            if args.kinesis and batch: # Added 'and batch' to avoid sending empty lists
                generator.send_to_kinesis(args.kinesis, batch)
            
            # Store data if output filename provided
            if args.output:
                all_data.extend(batch)
            
            # Wait for the next cycle
            time.sleep(args.frequency)
            count += 1
    
    except KeyboardInterrupt:
        logger.info("Data generation interrupted by user")
    
    # Save data to file if output specified
    if args.output and all_data:
        if args.format == 'json':
            save_to_file(all_data, args.output)
        else:
            save_to_csv(all_data, args.output)


if __name__ == "__main__":
    main()