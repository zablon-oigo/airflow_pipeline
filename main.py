import os
import json
import base64
import time
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()
base_url='https://api.twelvedata.com/'
api_key=os.getenv("API_KEY")
class KafkaProducerWrapper:
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv("KAFKA_API_KEY"),
            'sasl.password': os.getenv("KAFKA_API_SECRET"),
        })

    def send(self, topic, message):
        self.producer.produce(topic, message)
        self.producer.poll(0)

def fetch_stock_data():
    url=f"{base_url}stocks?apikey={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get("data", [])
    return []


def batch_send_to_kafka():
    producer = KafkaProducerWrapper()
    data = fetch_stock_data()
    batch_size = 100
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        batch_json = json.dumps(batch).encode('utf-8')
        encoded = base64.b64encode(batch_json).decode('utf-8')
        producer.send("finance1", encoded)
        print(f"Sent batch {i//batch_size + 1}")
        time.sleep(1)  

if __name__ == "__main__":
    batch_send_to_kafka()
    