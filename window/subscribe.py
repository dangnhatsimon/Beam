import os
import time
from google.cloud import pubsub_v1
from google.oauth2 import service_account


if __name__ == "__main__":
    credentials = service_account.Credentials.from_service_account_file("D:/Beam/credentials.json")

    subscription_path = "projects/streaming-445511/subscriptions/Topic4-sub"

    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

    def callback(message):
        print((f"Received {message}."))
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    while True:
        time.sleep(60)