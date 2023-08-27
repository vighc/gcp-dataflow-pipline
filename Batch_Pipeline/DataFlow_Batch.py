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
   element = element.split(';')
   element = {'Country':element[0],'TotalCases':element[1],'NewCases':element[2],'TotalDeaths':element[3],'NewDeaths':element[4],'TotalRecovered':element[5],'NewRecovered':element[6],'ActiveCases':element[7],'Serious_Critical':element[8],'TotalCases 1M':element[9],'Deaths':element[10],'TotalTests':element[11],'Tests':element[12],'Population':element[13],'Continent':element[14],'1 Caseevery X ppl':element[15],'1 Deathevery X ppl':element[16],'1 Testevery X ppl':element[17],'New Cases':element[18],'New Deaths':element[19],'Active Cases':element[20]}
   return element

def run(args):
    # project-id:dataset_id.table_id
    table_spec = args.project+':dataflow_dataset.covid_table'
    table_schema = 'Country:STRING, TotalCases:STRING, NewCases:STRING, TotalDeaths:STRING, NewDeaths:STRING, TotalRecovered:STRING, NewRecovered:STRING, ActiveCases:STRING, Serious_Critical:STRING, TotalCases 1M:STRING, Deaths:STRING, TotalTests:STRING, Tests:STRING, Population:STRING, Continent:STRING, 1 Caseevery X ppl:STRING, 1 Deathevery X ppl:STRING, 1 Testevery X ppl:STRING, New Cases:STRING, New Deaths:STRING, Active Cases:STRING'

    pipeline_options = MyPipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        (p
        | "Read Data" >> beam.io.ReadFromText(args.filename,skip_header_lines=1)
        | "Process Data" >> beam.Map(process_element)
        | "Load Data" >> beam.io.WriteToBigQuery(
                            table_spec,
                            schema=table_schema,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                                                )
        )

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("--filename", required=True)
  parser.add_argument("--project", required=True)
  args = parser.parse_args()
  run(args)