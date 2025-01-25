import os
import time
from google.cloud import pubsub_v1
from google.oauth2 import service_account

if __name__ == "__main__":

       # Replace  with your project id
    project = "streaming-445511"

    # Replace  with your pubsub topic
    pubsub_topic = "projects/streaming-445511/topics/Topic5"

    # Replace with your service account path
    path_service_account = "D:/Beam/credentials.json"

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

    # Replace  with your input file path
    input_file = "D:/Beam/mobile_games/mobile_game.txt"

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, "rb") as ifp:
        # skip header
        header = ifp.readline()

        # loop over each record
        for line in ifp:
            event_data = line   # entire line of input CSV is the message
            print(f"Publishing {event_data} to {pubsub_topic}")
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1)