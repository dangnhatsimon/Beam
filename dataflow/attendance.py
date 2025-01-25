import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse


parser = argparse.ArgumentParser()
parser.add_argument(
    "--input",
    dest="input",
    required=True,
    help="Input file to process."
)
parser.add_argument(
    "--output",
    dest="output",
    required=True,
    help="Output file to write results to."
)
path_args, pipeline_args = parser.parse_known_args()
inputs_pattern = path_args.input
outputs_pattern = path_args.output

options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options=options)

attendence_count = (
    p
    | "Read lines" >> beam.io.ReadFromText(inputs_pattern)
    | "Spli rows" >> beam.Map(lambda record: record.split(","))
    | "Get all accounts dept persons" >> beam.Filter(lambda record: record[3] == "Accounts")
    | "Pair each employee with 1" >> beam.Map(lambda record: ("Accounts, " + record[1], 1))
    | "Group and sum" >> beam.CombinePerKey(sum)
    | "Format results" >> beam.Map(lambda employee_count: str(employee_count))
    | "Write results" >> beam.io.WriteToText(outputs_pattern)
)

p.run()

# python dataflow/attendance.py --input gs://dataflow/input/dept_data.txt --output gs://dataflow/dataflow/output/part --runner DataflowRunner --project streaming-445511 --temp_location gs://dataflow/dataflow/temp --region us-central1 --staging_location gs://dataflow/dataflow/staging