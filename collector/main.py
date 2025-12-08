import os
import time
import sched
import httpx
import pika
import json
from dotenv import load_dotenv

load_dotenv()

apiKey = os.getenv("WEATHER_API_KEY")
rabbitmqIp = os.getenv("RABBITMQ_IP")

scheduler = sched.scheduler(time.time, time.sleep)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(rabbitmqIp or "localhost")
)
channel = connection.channel()

channel.queue_declare(queue="weather_report", durable=True)

def getWeather(appId):
    response = httpx.get(
        f"https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&appid={appId}"
    ).json()

    keysToExtract = {'timezone': None, 'current': {}, 'hourly': {'pop'}}

    weather = {}

    for key in keysToExtract:
        if key in response:
            if type(keysToExtract[key]) is type(None):
                weather[key] = response[key]
            elif type(keysToExtract[key]) is type({}):
                for i in response[key]:
                    weather[i] = response[key][i]
            else:
                for i in keysToExtract[key]:
                    weather[i] = response[key][0][i]


    return weather

def sendToQueue(function, key):
    response = json.dumps(function(key))
    channel.basic_publish(exchange='', routing_key="weather_report", body=response, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))

def repeatAfterInterval(scheduler, interval, action, args=()):
    action(args[0], args[1])
    scheduler.enter(
        interval, 1, repeatAfterInterval, (scheduler, interval, action, args)
    )


repeatAfterInterval(scheduler, 10, sendToQueue, (getWeather, apiKey))
scheduler.run()
