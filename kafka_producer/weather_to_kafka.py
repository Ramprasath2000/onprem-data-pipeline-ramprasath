import os
import requests
import json
from kafka import KafkaProducer

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("WEATHER_CITY", "London")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather-topic")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"

def fetch_weather():
    r = requests.get(URL)
    if r.status_code == 200:
        print("Fetched weather data")
        return r.json()
    else:
        print(f"Error: {r.status_code}, {r.text}")
        return None

def main():
    weather = fetch_weather()
    if weather:
        print("Pushing to Kafka...")
        producer.send(TOPIC, weather)
        producer.flush()

if __name__ == "__main__":
    main()
