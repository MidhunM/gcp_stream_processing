import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from google.cloud.bigtable import row
import sys
import pandas as pd
import json


PROJECT = 'serene-star-373712'
BUCKET  = 'pub_sub_gcp_bucket'
schema  = 'Index:string,Date:string,Open:float,High:float,Low:float,Close:float,Adj_Close:float,Volume:float,CloseUSD:float'

class Parse(beam.DoFn):

    def process(self, element):
        data = json.loads(element)
        return [{
                "Index": data['Index'],
                "Date": data['Date'],
                "Open": data['Open'],
                "High":data['High'],
                "Low": data['Low'],
                "Close": data['Close'],
                "Adj_Close": data['Adj_Close'],
                "Volume": data['Volume'],
                "CloseUSD": data['CloseUSD']
            }]

   

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
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:stock_data.stock_info_internal'.format(PROJECT), schema=schema)
   )


   p.run()

if __name__ == '__main__':
   run()