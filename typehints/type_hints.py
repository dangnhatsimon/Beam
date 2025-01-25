# import beam module
import apache_beam as beam

p = beam.Pipeline()


@beam.typehints.with_input_types(int)
class FilterEvensDoFn(beam.DoFn):
    def process(self, element):
        if element % 2 == 0:
            yield element

evens = ( p
         | beam.Create(["1", "2", "3"])
         | beam.ParDo(FilterEvensDoFn()) 
        )

p.run()


p = beam.Pipeline()

evens = ( p 
         | beam.Create(["one", "two", "three"]) 
         | beam.Filter(lambda x: x % 2 == 0).with_input_types(int) 
        )

p.run()