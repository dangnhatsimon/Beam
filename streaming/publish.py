import os
import time
from google.cloud import pubsub_v1

if __name__ == "__main__":
    project_id = "streaming-445511"
    pubsub_topic = "projects/streaming-445511/topics/Topic1"
    path_service_account = "D:/Beam/credentials.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

    input_file = "D:/Beam/streaming/data.txt"

    publisher = pubsub_v1.PublisherClient()

    with open(input_file, "rb") as ifp:
        header = ifp.readline()

        for line in ifp:
            event_data = line
            print(f"Publishing {event_data} to {pubsub_topic}")
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1)