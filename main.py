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

