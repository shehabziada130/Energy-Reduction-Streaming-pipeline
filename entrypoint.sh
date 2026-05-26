echo "Running Docker" 

docker-compose up -d 

echo "Starting kafka"

cd Kafka
python3  kafka_stream.py &

python3 ../Spark/kafka_to_features.py


