from kafka import KafkaProducer
from binance.client import Client
import json
import time

# Binance API keys
api_key = 'Xv8AxoJ7dLpYJc2gvGW91---FAKEKEY'
api_secret = 'areFQCJNAgHUYBpX2yUO3u2XJhOW----TYPICALLY USE GETPASS4'

# Initialize Binance client
client = Client(api_key, api_secret)

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# List of symbols and their respective topics
symbol_topic_map = {
    "BTCUSDT": "BTC",
    "AVAXUSDT": "AVA",
    "BNBUSDT": "BNB",
    "SOLUSDT": "SOL"
}

def get_price(symbol):
    return client.get_symbol_ticker(symbol=symbol)['price']

while True:
    for symbol, topic in symbol_topic_map.items():
        price = get_price(symbol)
        data = {
            'symbol': symbol,
            'price': price,
            'event_time_ms': int(time.time())
        }
        print(f"Sending {data} to topic {topic}")
        producer.send(topic, value=data)
    
    # Sleep for 0.5 seconds before the next fetch
    time.sleep(0.5)
