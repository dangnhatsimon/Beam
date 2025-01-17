import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount

# Replace with your service account path
service_account_path = ''

print("Service account file : ", service_account_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

# Replace with your input subscription id
input_subscription = ''

# Replace with your output subscription id
output_topic = ''

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

def encode_byte_string(element):
   print element
   element = str(element)
   return element.encode('utf-8')

def calculateProfit(elements):
  buy_rate = elements[5]
  sell_price = elements[6]
  products_count = int(elements[4])
  profit = (int(sell_price) - int(buy_rate)) * products_count
  elements.append(str(profit))
  return elements

pubsub_data= (
                p 
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription)  
                # STR_2,Mumbai,PR_265,Cosmetics,8,39,66/r/n

                | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))          # STR_2,Mumbai,PR_265,Cosmetics,8,39,66,
                | 'Split Row' >> beam.Map(lambda row : row.split(','))                             # [STR_2,Mumbai,PR_265,Cosmetics,8,39,66,]
                | 'Filter By Country' >> beam.Filter(lambda elements : (elements[1] == "Mumbai" or elements[1] == "Bangalore"))
                | 'Create Profit Column' >> beam.Map(calculateProfit)                              # [STR_2,Mumbai,PR_265,Cosmetics,8,39,66,27]
                | 'Form Key Value pair' >> beam.Map(lambda elements : (elements[0], int(elements[7])))  # STR_2 27
                | 'Window' >> beam.WindowInto(window.FixedWindows(20))
                | 'Sum values' >> beam.CombinePerKey(sum)
                | 'Encode to byte string' >> beam.Map(encode_byte_string)  #Pubsub takes data in form of byte strings 
                | 'Write to pus sub' >> beam.io.WriteToPubSub(output_topic)
	             )

result = p.run()
result.wait_until_finish()