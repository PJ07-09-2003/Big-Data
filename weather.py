import os
import requests
import time
import random
import json
import socket
import fastavro
import io

# API credentials and base query URL
API_KEY = "fbc3424962832e0d15c034b13c5e4836"
CITIES = [
    "Vijayawada", "Visakhapatnam", "Guntur", "Nellore", "Kakinada", "Rajahmundry",
    "Tirupati", "Khammam", "Anantapur", "Chittoor"
]  # List of cities in Andhra Pradesh

API_URL = "http://api.weatherstack.com/current?access_key={}&query={}".format(API_KEY, "{}")

# Flume hostname and port for data streaming
FLUME_HOST = "localhost"  # Change this to your Flume server's IP
FLUME_PORT = 41414  # Port where Flume listens for data

# Avro schema path
SCHEMA_PATH = '/path/to/weather_data.avsc'

# Function to fetch weather data for a given city
def fetch_weather_data(city):
    response = requests.get(API_URL.format(city))

    if response.status_code == 200:
        data = response.json()
        if "error" not in data:
            weather_info = {
                "city": data['location']['name'],
                "region": data['location']['region'],
                "country": data['location']['country'],
                "temperature": data['current']['temperature'],
                "weather": data['current']['weather_descriptions'][0]
            }

            # Send data to Flume
            send_to_flume(weather_info)
        else:
            print("API Error:", data["error"]["info"])
    else:
        print("Failed to retrieve data (HTTP {})".format(response.status_code))

# Function to serialize data to Avro format
def serialize_to_avro(data, schema_path):
    with open(schema_path, "r") as schema_file:
        schema = fastavro.schema.load_schema(schema_file)
    
    # Create an in-memory buffer for Avro data
    buf = io.BytesIO()
    fastavro.writer(buf, schema, [data])
    return buf.getvalue()  # Return the serialized Avro data

# Function to send data to Flume via UDP
def send_to_flume(data):
    avro_data = serialize_to_avro(data, SCHEMA_PATH)
    
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.sendto(avro_data, (FLUME_HOST, FLUME_PORT))
        print(f"Data sent to Flume: {data}")

# Fetch data for a random set of cities every 10 seconds (batch-wise)
try:
    while True:
        # Randomly select 10 cities from the list
        random_cities = random.sample(CITIES, 10)  # Pick 10 cities at random

        for city in random_cities:
            fetch_weather_data(city)
            time.sleep(10)  # Wait for 10 seconds before fetching data for the next city

        print("Batch of cities processed, waiting to fetch new batch.")

except KeyboardInterrupt:
    print("Data fetching stopped.")
