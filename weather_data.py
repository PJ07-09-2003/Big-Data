import urllib
import json
import time

# (Your WeatherStack API key - replace with yours)
API_KEY = "c1c516e025b253a6fe3a33411344342f"

# Locations to fetch weather data for
locations = ["New York", "London", "Tokyo", "Mumbai", "Sydney"]

# Output file for Flume
output_file = "/tmp/weather_data.txt"

# Function to fetch weather data using urllib
def fetch_weather(location):
    url = "http://api.weatherstack.com/current?access_key=" + API_KEY + "&query=" + location
    response = urllib.urlopen(url)
    if response.getcode() == 200:
        data = json.loads(response.read())
        if "current" in data:
            return {
                "location": data["location"]["name"],
                "temperature": data["current"]["temperature"],
                "humidity": data["current"]["humidity"],
                "timestamp": data["location"]["localtime"]
            }
    return None

# Main loop to fetch and write data
while True:
    with open(output_file, "a") as file:
        for location in locations:
            weather = fetch_weather(location)
            if weather:
                # Convert the data to a line string format
                line = "{},{},{},{}\n".format(weather['timestamp'], weather['location'], weather['temperature'], weather['humidity'])
                file.write(line)
    time.sleep(60)  # Fetch data every 60 seconds
