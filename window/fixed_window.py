import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount

# Replace with your service account path
service_account_path = "D:/Beam/credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

# Replace with your input subscription id
input_subscription = "projects/streaming-445511/subscriptions/Topic1-sub"

# Replace with your output subscription id
output_topic = "projects/streaming-445511/topics/Topic2"

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)


def encode_byte_string(element):
    element = str(element)
    return element.encode('utf-8')


def custom_timestamp(elements):
    unix_timestamp = elements[7]
    return window.TimestampedValue(elements, int(unix_timestamp))


def calculateProfit(elements):
    buy_rate = elements[5]
    sell_price = elements[6]
    products_count = int(elements[4])
    profit = (int(sell_price) - int(buy_rate)) * products_count
    elements.append(str(profit))
    return elements


pubsub_data= (
    p 
    | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription=input_subscription, timestamp_attribute=1553578219)  
    # STR_2,Mumbai,PR_265,Cosmetics,8,39,66,1553578219/r/n
    | 'Decode byte string' >> beam.Map(lambda data: data.decode('utf-8'))
    | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))          # STR_2,Mumbai,PR_265,Cosmetics,8,39,66,1553578219
    | 'Split Row' >> beam.Map(lambda row : row.split(','))                             # [STR_2,Mumbai,PR_265,Cosmetics,8,39,66,1553578219]
    | 'Filter By Country' >> beam.Filter(lambda elements : (elements[1] == "Mumbai" or elements[1] == "Bangalore"))
    | 'Create Profit Column' >> beam.Map(calculateProfit)                              # [STR_2,Mumbai,PR_265,Cosmetics,8,39,66,1553578219,27]
    | 'Apply custom timestamp' >> beam.Map(custom_timestamp) 
    | 'Form Key Value pair' >> beam.Map(lambda elements : (elements[0], int(elements[8])))  # STR_2 27
    | 'Window' >> beam.WindowInto(window.FixedWindows(20))
    | 'Sum values' >> beam.CombinePerKey(sum)
    | 'Encode to byte string' >> beam.Map(encode_byte_string)  #Pubsub takes data in form of byte strings 
    | 'Write to pus sub' >> beam.io.WriteToPubSub(output_topic)
)

result = p.run()
result.wait_until_finish()