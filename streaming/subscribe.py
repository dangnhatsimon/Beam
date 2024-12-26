from google.cloud import pubsub_v1
import os
import time

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:/Beam/credentials.json"

    subscription_path = "projects/streaming-445511/subscriptions/Topic2-sub"
    subscriber = pubsub_v1.SubscriberClient()

    def callback(message):
        print(f"Received {message}.")
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    while True:
        time.sleep(60)