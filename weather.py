import os
import requests
import time
import random
import json
import socket

# API credentials and base query URL
API_KEY = "fbc3424962832e0d15c034b13c5e4836"
CITIES = [
    "Vijayawada", "Visakhapatnam", "Guntur", "Nellore", "Kakinada", "Rajahmundry",
    "Tirupati", "Khammam", "Anantapur", "Chittoor"
]  # List of cities in Andhra Pradesh

API_URL = "http://api.weatherstack.com/current?access_key={}&query={}".format(API_KEY, "{}")

# Flume hostname and port for data streaming
FLUME_HOST = "localhost"  # Change this to your Flume server's IP
FLUME_PORT = 41414  # Change this to the Flume port where your source is listening

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

            # Send data to Flume via UDP (or other protocol you configure)
            send_to_flume(weather_info)
        else:
            print("API Error:", data["error"]["info"])
    else:
        print("Failed to retrieve data (HTTP {})".format(response.status_code))


# Send data to Flume
def send_to_flume(data):
    # Establish UDP socket to send data to Flume
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        # Convert the data to a JSON string
        message = json.dumps(data)
        s.sendto(message.encode(), (FLUME_HOST, FLUME_PORT))
        print(f"Data sent to Flume: {message}")


# Fetch data for a random set of cities every 10 seconds (batch-wise)
try:
    while True:
        # Randomly select 10 cities from the list
        random_cities = random.sample(CITIES, 10)  # Pick 10 cities at random

        for city in random_cities:
            fetch_weather_data(city)
            time.sleep(10)  # Wait for 10 seconds before fetching data for the next city

        # After a batch of 10 cities, repeat the process or break as needed
        print("Batch of cities processed, waiting to fetch new batch.")

except KeyboardInterrupt:
    print("Data fetching stopped.")
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

# Spark configuration
conf = SparkConf().setAppName("WeatherDataFlume")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)  # 10 seconds batch interval

# Flume source (where Flume sends data)
flume_stream = FlumeUtils.createStream(ssc, "localhost", 12345)  # Replace with your Flume sink's hostname and port

# Function to process the weather data
def process_weather_data(record):
    # Parse the record (Flume sends data as a tuple, with the second element being the message)
    message = record[1]
    weather_info = json.loads(message)
    print(weather_info)

# Process the incoming weather data stream
flume_stream.foreachRDD(lambda rdd: rdd.foreach(process_weather_data))

# Start streaming
ssc.start()
ssc.awaitTermination()
    
    
