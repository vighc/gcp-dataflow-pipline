import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class MyPipelineOptions(PipelineOptions):
   @classmethod
   def _add_argparse_args(cls, parser):
       # Add your pipeline-specific command line arguments here
       pass

def process_element(element):
   # Define your data processing logic here
   element = element.decode('utf-8')
   element=json.loads(element)
   return element

def run(args):
    # project-id:dataset_id.table_id
    table_spec = args.project+':dataflow_dataset.covid_table'
    table_schema = 'Country:STRING, TotalCases:STRING, NewCases:STRING, TotalDeaths:STRING, NewDeaths:STRING, TotalRecovered:STRING, NewRecovered:STRING, ActiveCases:STRING, Serious_Critical:STRING, TotalCases 1M:STRING, Deaths:STRING, TotalTests:STRING, Tests:STRING, Population:STRING, Continent:STRING, 1 Caseevery X ppl:STRING, 1 Deathevery X ppl:STRING, 1 Testevery X ppl:STRING, New Cases:STRING, New Deaths:STRING, Active Cases:STRING'
    
    pipeline_options = MyPipelineOptions(streaming=True)
    with beam.Pipeline(options=pipeline_options) as p:
        (p
        | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription="projects/"+args.project+"/subscriptions/dataflow-pubsub-sub")
        | "Process Data" >> beam.Map(process_element)
        | "Load Data" >> beam.io.WriteToBigQuery(
                            table_spec,
                            schema=table_schema,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                                                )
        )

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("--project", required=True)
  args = parser.parse_args()
  run(args)