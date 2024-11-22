from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
import json

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
