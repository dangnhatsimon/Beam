import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import Windowing

service_account_path = "D:/Beam/streaming/credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

input_subscription = "projects/streaming-445511/subscriptions/Topic1-sub"

output_topic = "projects/streaming-445511/topics/Topic2"

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

output_file = "outputs/part"

pubsub_data = (
    p
    | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
    | "Write to Pub/Sub" >> beam.io.WriteToPubSub(output_topic)
)

result = p.run()
result.wait_until_finish()