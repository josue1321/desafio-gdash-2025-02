import os
import time
import sched
import requests
import pika
from dotenv import load_dotenv

load_dotenv()

apiKey = os.getenv("WEATHER_API_KEY")
rabbitmqIp = os.getenv("RABBITMQ_IP")

scheduler = sched.scheduler(time.time, time.sleep)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(rabbitmqIp or "localhost")
)
channel = connection.channel()


def getWeather(apiKey):
    response = requests.get(
        f"https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&appid={apiKey}"
    )

    return response


def repeatAfterInterval(scheduler, interval, action, args=()):
    getWeather(args)
    scheduler.enter(
        interval, 1, repeatAfterInterval, (scheduler, interval, action, args)
    )


repeatAfterInterval(scheduler, 10, getWeather, (apiKey))
scheduler.run()
