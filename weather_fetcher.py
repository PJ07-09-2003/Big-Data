import os
import requests
import time
import random

# Log file path (for Ambari Sandbox or local path in GitHub repository)
LOG_FILE = "/home/maria_dev/log/weather_data.log"  # Modify this path based on your setup

# Ensure the log directory exists
log_directory = "/home/maria_dev/log"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)  # Create the directory if it doesn't exist

# API credentials and base query URL
API_KEY = "fbc3424962832e0d15c034b13c5e4836"
CITIES = [
    "Vijayawada", "Visakhapatnam", "Guntur", "Nellore", "Kakinada", "Rajahmundry",
    "Tirupati", "Khammam", "Anantapur", "Chittoor"
]  # List of cities in Andhra Pradesh

API_URL = "http://api.weatherstack.com/current?access_key={}&query={}".format(API_KEY, "{}")

# Function to fetch weather data for a given city
def fetch_weather_data(city):
    response = requests.get(API_URL.format(city))

    if response.status_code == 200:
        data = response.json()
        if "error" not in data:
            # Print relevant weather information
            print("City: {}".format(data['location']['name']))
            print("Region: {}".format(data['location']['region']))
            print("Country: {}".format(data['location']['country']))
            print("Temperature: {}°C".format(data['current']['temperature']))
            print("Weather: {}".format(data['current']['weather_descriptions'][0]))

            # Write the same information to the log file
            with open(LOG_FILE, "a") as log:
                log.write("City: {}\n".format(data['location']['name']))
                log.write("Region: {}\n".format(data['location']['region']))
                log.write("Country: {}\n".format(data['location']['country']))
                log.write("Temperature: {}°C\n".format(data['current']['temperature']))
                log.write("Weather: {}\n".format(data['current']['weather_descriptions'][0]))
                log.write("\n")
        else:
            # Print API error details
            print("API Error:", data["error"]["info"])
    else:
        print("Failed to retrieve data (HTTP {})".format(response.status_code))


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

        # You can choose to exit the loop after one complete batch or run indefinitely.
        # To stop after one batch, break the loop:
        # break

except KeyboardInterrupt:
    print("Data fetching stopped.")
