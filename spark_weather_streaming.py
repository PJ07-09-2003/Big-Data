from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
import json

def parse_weather_data(line):
    try:
        data = json.loads(line)
        main = data.get("main", {})
        temp = main.get("temp", None)
        humidity = main.get("humidity", None)
        weather = data.get("weather", [{}])[0].get("description", "No description")
        return f"Temperature: {temp}, Humidity: {humidity}, Weather: {weather}"
    except Exception as e:
        return f"Error parsing data: {e}"

if __name__ == "__main__":
    sc = SparkContext(appName="StreamingFlumeWeatherData")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)

    flumeStream = FlumeUtils.createStream(ssc, "localhost", 9092)
    lines = flumeStream.map(lambda x: x[1])
    weather_info = lines.map(parse_weather_data)

    weather_info.pprint()

    ssc.checkpoint("/home/maria_dev/checkpoint")
    ssc.start()
    ssc.awaitTermination()
