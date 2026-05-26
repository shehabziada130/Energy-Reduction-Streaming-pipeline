from kafka import KafkaProducer
import csv
import time
import json

topic_name = 'spark_iber_data_stream'
kafka_broker = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_file = '../Data/2019Floor3.csv'

# Stream data row by row
with open(csv_file, 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        producer.send(topic_name, value=row)  # Send each row as a JSON message
        print(f"Sent: {row}")  
        time.sleep(1)  # Simulate real-time streaming delay

producer.close()
