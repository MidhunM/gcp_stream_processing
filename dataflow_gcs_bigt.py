import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from google.cloud.bigtable import row
import sys
import pandas as pd
import json
import datetime

PROJECT = 'serene-star-373712'
BUCKET  = 'pub_sub_gcp_bucket'

class Parse(beam.DoFn):

    def process(self, element):
        data = json.loads(element)
        direct_row = row.DirectRow(row_key=str(data['Index']))

        direct_row.set_cell(
        'stock_val', "Open",str(data['Open']), timestamp = datetime.datetime.utcnow())
        direct_row.set_cell(
        'stock_val', "High",str(data['High']), timestamp = datetime.datetime.utcnow())
        direct_row.set_cell(
        'stock_val',"Low", str(data['Low']), timestamp   = datetime.datetime.utcnow())

        ## Create column family in bigtable
        return [direct_row]

   

def run():
   pipeline_options = PipelineOptions(
      project=PROJECT,
      staging_location='gs://{0}/temp/'.format(BUCKET),
      temp_location='gs://{0}/temp/'.format(BUCKET),
      runner='DirectRunner',
      region='asia-south2'
   )

   p = beam.Pipeline(options=pipeline_options)

   (p
      | 'ReadFromGCS' >> beam.io.textio.ReadFromText('gs://{0}/stock_data.json'.format(BUCKET))
      | 'ParseCSV' >> beam.ParDo(Parse())
      | 'WriteToBigTable' >> WriteToBigTable(project_id  = PROJECT,
                                             instance_id = 'serene-star-373712',
                                             table_id    = 'stock_info')
   )


   p.run()

if __name__ == '__main__':
   run()