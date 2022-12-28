# Import necessary modules from Apache Beam
import os
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.pvalue import PCollection
from apache_beam.transforms import PTransform

#Service
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "../service-account.json"

table = "dt-data-analytics:test.sample"
table_schema = "columna1:INTEGER, columna2:STRING, columna3:STRING"



# Define a transform to parse the CSV data and convert it to a format that can be written to BigQuery
class ParseData(PTransform):
  def expand(self, pcollection):
    return (pcollection
            | 'Split Lines' >> beam.Map(lambda x: x.split(','))
            | 'Create Dict' >> beam.Map(lambda x: {'columna1': x[0], 'columna2': x[1], 'columna3': x[2]})
           )

# Define a transform to write the parsed data to BigQuery
class WriteToBQ(PTransform):
    def expand(self, pcollection):
        return (pcollection
                | 'Write to BigQuery' >> WriteToBigQuery(table,
                                                        schema=table_schema,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, #delete existing rows.
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, #create if does not exist. 
                                                        custom_gcs_temp_location="gs://pruebas-dt-data-analytics/tmp/")
            )

def main():
    parser = argparse.ArgumentParser(description="Read Data from BigQuery")
    parser.version="1.0"

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)


def run_pipeline(our_args, beam_args):

    options  = {"project": "dt-data-analytics",
                "runner": 'DataFlow',  # <------ Runtine
                'staging_location':"gs://pruebas-dt-data-analytics/tmp/",
                "temp_location": "gs://pruebas-dt-data-analytics/tmp/",
                "region": "us-east1",
                "experiments": ['shuffle_mode=service'], # <--- reduce resource usage,  only for batch jobs
                } 

    pipeline_options = beam.pipeline.PipelineOptions(beam_args,**options)
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Specify the source of the data in Cloud Storage
        data_source = pipeline | 'Read Data Cloud Storage' >> ReadFromText('gs://pruebas-dt-data-analytics/data/sample.csv', skip_header_lines = 1)

        # Apply the transform to the source data
        parsed_data = data_source | 'Parse Data' >> ParseData()


        # Apply the transform to write the data to BigQuery
        _ = parsed_data | 'Write to BigQuery' >> WriteToBQ()


if __name__ == "__main__":
    main()