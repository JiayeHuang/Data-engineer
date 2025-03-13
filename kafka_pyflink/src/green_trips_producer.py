import csv
import json
from time import time
from kafka import KafkaProducer

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("Connection Status: ", producer.bootstrap_connected())

    csv_file = '../data/green_tripdata_2019-10.csv'
    desired_cols = {'lpep_pickup_datetime', 'lpep_dropoff_datetime', \
                    'PULocationID', 'DOLocationID', 'passenger_count', \
                    'trip_distance', 'tip_amount'}
    topic_name = 'green-trips'

    t0 = time()

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-data"
            message = {key: row[key] for key in desired_cols if key in row}
            producer.send(topic_name, value=message)

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()

    t1 = time()
    print(f'took {(t1 - t0):.2f} seconds to send dataset and flush')


if __name__ == "__main__":
    main()
