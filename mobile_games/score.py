import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly
from datetime import datetime

# Replace with your service account path
service_account_path = "D:/Beam/credentials.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

# Replace with your input subscription id
input_subscription = "projects/streaming-445511/subscriptions/Topic5-sub"

# Replace with your output subscription id
output_topic_player = "projects/streaming-445511/topics/Topic6"
output_topic_team = "projects/streaming-445511/topics/Topic7"

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)


def custom_timestamp(elements):
    unix_timestamp = elements[16].rstrip().lstrip()
    return window.TimestampedValue(elements, int(unix_timestamp))


def encode_byte_string(element):
    print(element)
    element = str(element)
    return element.encode("utf-8")


def player_pair(element_list):
    # key-value pair of (player_id, 1)
    return element_list[1], 1


def score_pair(element_list):
    # key-value pair of (team_id, 1)
    return element_list[3], 1


pubsub_data = (
                p
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
                | 'Parse data' >> beam.Map(lambda element: element.decode("utf-8").split(","))
                | 'Apply custom timestamp' >> beam.Map(custom_timestamp)
              )

player_score = (
                pubsub_data
                | 'Form k,v pair of (player_id, 1)' >> beam.Map(player_pair)
                | 'Window for player' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(10)), accumulation_mode=AccumulationMode.ACCUMULATING) 
                | 'Group players and their score' >> beam.CombinePerKey(sum)
                | 'Encode player info to byte string' >> beam.Map(encode_byte_string)
                | 'Write player score to pub sub' >> beam.io.WriteToPubSub(output_topic_player)
              )

team_score = (
                pubsub_data
                | 'Form k,v pair of (team_score, 1)' >> beam.Map(score_pair)
                | 'Window for team' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(10)), accumulation_mode=AccumulationMode.ACCUMULATING)
                | 'Group teams and their score' >> beam.CombinePerKey(sum)
                | 'Encode teams info to byte string' >> beam.Map(encode_byte_string)
                | 'Write team score to pub sub' >> beam.io.WriteToPubSub(output_topic_team)
              )

result = p.run()
result.wait_until_finish()
