from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('quickstart-events',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my_consumer_group',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def receive_message():
    for message in consumer:
        print(f"Received message: {message.value}")

if __name__ == '__main__':
    while True:
        receive_message()
