import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        # Read data from text file
        data = (
            p
            | 'ReadFromText' >> beam.io.ReadFromText('input.txt')
            | 'ParseData' >> beam.ParDo(ParseFn())
            | 'GroupByCustomer' >> beam.GroupByKey()
            | 'CountPerCustomer' >> beam.CombinePerKey(sum)
            | 'WriteOutput' >> beam.io.WriteToText('output.txt')
        )

class ParseFn(beam.DoFn):
    def process(self, element):
        try:
            parts = element.split(',')
            customer_id = parts[0]
            amount = int(parts[1])
            yield (customer_id, amount)
        except Exception:
            pass

if __name__ == '__main__':
    run()
