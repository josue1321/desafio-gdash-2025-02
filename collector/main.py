import os
import time
import sched
import httpx
import pika
import json
import logging


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_weather(appId):
    try:
        response_obj = httpx.get(
            f"https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&appid={appId}",
            timeout=10.0,
        )

        response_obj.raise_for_status()
        response = response_obj.json()
    except httpx.RequestError as e:
        logging.error(f"Network error occurred: {e}")
        return None
    except httpx.HTTPStatusError as e:
        logging.error(f"API error: {e}")
        return None
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON response")
        return None

    keysToExtract = {"timezone": None, "current": {}, "hourly": {"pop"}}

    weather = {}

    for key in keysToExtract:
        if key in response:
            try:
                if type(keysToExtract[key]) is type(None):
                    weather[key] = response[key]
                elif type(keysToExtract[key]) is type({}):
                    for i in response[key]:
                        weather[i] = response[key][i]
                else:
                    if len(response[key]) > 0:
                        for i in keysToExtract[key]:
                            weather[i] = response[key][0][i]
            except (KeyError, IndexError, TypeError) as e:
                logging.error(f"Error parsing data for key {key}: {e}")

    return weather


def send_to_queue(function, key, channel):
    data = function(key)

    if data is None:
        logging.warning("No data fetched, skipping queue publish.")
        return

    try:
        message = json.dumps(data)

        channel.basic_publish(
            exchange="",
            routing_key="weather_report",
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )

        logging.info("Message published to the queue")
    except pika.exceptions.AMQPError as e:
        logging.error(f"RabbitMQ Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error publishing: {e}")


def repeat_after_interval(scheduler, interval, action, args=()):
    try:
        action(*args)
    except Exception as e:
        logging.error(f"Job failed, but rescheduling: {e}")

    scheduler.enter(
        interval, 1, repeat_after_interval, (scheduler, interval, action, args)
    )


def connect_to_rabbitmq(
    rabbitmq_host, rabbitmq_user, rabbitmq_pass, rabbitmq_heartbeat
):
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbitmq_host,
                    port=5672,
                    credentials=pika.PlainCredentials(rabbitmq_user, rabbitmq_pass),
                    heartbeat=rabbitmq_heartbeat,
                )
            )
            channel = connection.channel()

            return channel
        except pika.exceptions.AMQPConnectionError as e:
            logging.error(f"Error trying to connect: {e}")
            time.sleep(0.1)


def main():
    apiKey = os.getenv("WEATHER_API_KEY")
    rabbitmq_host = os.getenv("RABBITMQ_HOST") or "localhost"
    rabbitmq_user = os.getenv("RABBITMQ_USER") or "guest"
    rabbitmq_pass = os.getenv("RABBITMQ_PASS") or "guest"

    scheduler = sched.scheduler(time.time, time.sleep)

    interval = 1 * 60 * 60

    channel = connect_to_rabbitmq(rabbitmq_host, rabbitmq_user, rabbitmq_pass, interval)
    channel.queue_declare(queue="weather_report", durable=True)

    repeat_after_interval(
        scheduler, interval, send_to_queue, (get_weather, apiKey, channel)
    )
    scheduler.run()


if __name__ == "__main__":
    main()
