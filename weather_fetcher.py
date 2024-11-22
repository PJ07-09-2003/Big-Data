import requests
import time

def fetch_weather_data():
    api_key = "c1c516e025b253a6fe3a33411344342f"
    location = "London"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={location}&appid={api_key}"

    while True:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                with open('/home/maria_dev/spool/weather.log', 'a') as file:
                    file.write(response.text + "\n")
            else:
                print(f"Failed to fetch data: {response.status_code}")
        except Exception as e:
            print(f"Error fetching weather data: {e}")

        time.sleep(60)

if __name__ == "__main__":
    fetch_weather_data()
