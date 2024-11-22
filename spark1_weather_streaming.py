import requests
import time
import os

# API details
API_KEY = "c1c516e025b253a6fe3a33411344342f"
CITY = "London"  # Change this to your desired city
API_URL = "https://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units=metric".format(CITY, API_KEY)

# Log file path
LOG_FILE = "/home/maria_dev/spool/weather.log"

def fetch_weather():
    """
    Fetch weather data from the API.
    """
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            log_entry = {
                "main": {
                    "temp": data["main"]["temp"],
                    "humidity": data["main"]["humidity"]
                },
                "weather": data["weather"]
            }
            return log_entry
        else:
            return {"error": "API request failed with status code {}".format(response.status_code)}
    except Exception as e:
        return {"error": str(e)}

def write_to_log(log_entry):
    """
    Append weather data to the log file.
    """
    with open(LOG_FILE, "a") as file:
        file.write(str(log_entry) + "\n")

if __name__ == "__main__":
    # Create the directory if it doesn't exist
    if not os.path.exists(os.path.dirname(LOG_FILE)):
        os.makedirs(os.path.dirname(LOG_FILE))

    print("Generating log file. Press Ctrl+C to stop.")
    try:
        while True:
            log_entry = fetch_weather()
            write_to_log(log_entry)
            print("Logged data: {}".format(log_entry))
            time.sleep(5)  # Fetch data every 5 seconds
    except KeyboardInterrupt:
        print("Log file generation stopped.")
