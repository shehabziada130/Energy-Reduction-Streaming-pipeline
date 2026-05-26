# kafka_to_features.py
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from kafka import KafkaConsumer
from json import loads
import json

if __name__ == "__main__":
    # Define the features list
    features = ['z1_AC2(kW)', 'z1_AC3(kW)', 'z1_Plug(kW)', 'z1_S1(RH%)', 'z1_S1(lux)',
     'z2_AC1(kW)', 'z2_Light(kW)', 'z2_Plug(kW)', 'z2_S1(RH%)', 'z2_S1(lux)', 'z3_Light(kW)',
      'z3_Plug(kW)', 'z4_AC1(kW)','z4_Light(kW)', 'z4_Plug(kW)', 'z4_S1(RH%)', 'z4_S1(lux)',
       'z5_AC1(kW)', 'z5_Light(kW)', 'z5_Plug(kW)','z5_S1(RH%)']

    topic = 'spark_iber_data_stream'
    
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        api_version=(0, 10),
        enable_auto_commit=True,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    print("Connection to Kafka established")
    
    # Spark session
    spark = SparkSession.builder \
        .appName("KafkaToFeatures") \
        .getOrCreate()
    
    for message in consumer:
        data = message.value
        
        # Fill null values with 0.0 and filter to only keep desired features
        filtered_data = {feature: float(data.get(feature, 0.0)) for feature in features}
        
        # Save filtered data row-by-row to a temporary JSON file
        with open('filtered_data.json', 'a') as f:
            json.dump(filtered_data, f)
            f.write('\n')

        print(f"Filtered data saved: {filtered_data}")
