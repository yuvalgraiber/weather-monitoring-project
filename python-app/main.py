import requests
import pika
import json
import time
import os
from datetime import datetime

# Environment Variables Configuration
CITY = os.getenv("TARGET_CITY", "Mestia") # Targeted destination for upcoming treks
API_KEY = os.getenv("OPENWEATHER_API_KEY")
RABBITMQ_HOST = 'rabbitmq' # Internal Docker DNS name

def get_weather():
    """Fetch real-time weather data from OpenWeatherMap API."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def send_to_queue(data):
    """Inject millisecond-precision timestamp and publish to RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    
    # Ensure queue is durable to prevent data loss
    channel.queue_declare(queue='weather_queue', durable=True)
    
    # Requirement: Millisecond precision timestamp
    # Generating ISO8601 format: YYYY-MM-DDTHH:mm:ss.sssZ
    data['ingestion_timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    channel.basic_publish(
        exchange='',
        routing_key='weather_queue',
        body=json.dumps(data),
        properties=pika.BasicProperties(
            delivery_mode=2, # Persistent message
        )
    )
    
    print(f" [x] Weather data for {CITY} sent at {data['ingestion_timestamp']}")
    connection.close()

if __name__ == "__main__":
    print(f"Service started. Monitoring weather in {CITY}...")
    while True:
        try:
            weather_payload = get_weather()
            send_to_queue(weather_payload)
        except Exception as e:
            print(f"Runtime Error: {e}")
        
        # Sampling frequency: Once per hour
        time.sleep(3600)