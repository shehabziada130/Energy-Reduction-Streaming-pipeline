import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from influxdb_client import InfluxDBClient, Point, WritePrecision
import joblib
import json
import numpy as np
from influxdb_client.client.write_api import SYNCHRONOUS

# Load the pre-trained pipeline containing both StandardScaler and XGBoost model
with open('../../Classification_model/classification_pipeline.pkl', 'rb') as file:
    pipeline = joblib.load(file)

# InfluxDB connection setup
org = "elte"
username = 'admin'
password = 'admin'
database = 'iber'
retention_policy = 'autogen'
bucket = f'{database}/{retention_policy}'

with InfluxDBClient(url="http://localhost:8086", token=f'{username}:{password}', org='-', bucket=bucket) as client:
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Spark session
    spark = SparkSession.builder \
        .appName("PredictAndStoreToInflux") \
        .getOrCreate()

    # Function to check if new data is appended to the JSON file
    def read_new_data(file_path, last_line_position):
        new_data = []
        try:
            with open(file_path, 'r') as file:
                file.seek(last_line_position)  # Start reading from the last known position
                lines = file.readlines()
                if lines:
                    new_data = [json.loads(line) for line in lines]
                    last_line_position = file.tell()  # Update last read position
        except FileNotFoundError:
            pass
        return new_data, last_line_position

    # Read filtered data from the temporary JSON file and process it
    def process_and_store_data(file_path):
        last_line_position = 0  # Initially, set position to the start of the file
        no_new_data_count = 0
        while True:
            # Read new data from the file
            new_data, last_line_position = read_new_data(file_path, last_line_position)

            if new_data:
                no_new_data_count = 0  # Reset counter if new data is found
                for data in new_data:
                    # Extract feature values
                    feature_values = [data[feature] for feature in data.keys()]
                    feature_array = np.array([feature_values])

                    # Predict using the loaded pipeline (StandardScaler + XGBoost model)
                    prediction = pipeline.predict(feature_array)[0]

                    # Add prediction to the original data
                    data['prediction'] = prediction  # Store 1 for abnormal, 0 for normal

                    # Prepare the data for storage in InfluxDB
                    point = Point("abnormal_data_xgb").tag("bucket", bucket)
                    for key, value in data.items():
                        try:
                            # Convert value to float if possible
                            point = point.field(key, float(value) if value is not None else 0.0)
                        except ValueError:
                            # Store as string if conversion fails
                            point = point.field(key, str(value))

                    # Write the point to InfluxDB
                    write_api.write(bucket=bucket, org=org, record=point)
                    print(f"Data with prediction sent to InfluxDB: {point}")

            else:
                no_new_data_count += 1

            # Wait for 10 seconds before checking for new data again
            time.sleep(2)

            # Exit if no new data was found for the last 10 seconds
            if no_new_data_count > 0:
                print("No new data for the last 10 seconds. Exiting.")
                break

    # Start processing and storing data
    file_path = '../../Kafka/filtered_data.json'
    process_and_store_data(file_path)
