import os
import argparse
import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions

#Service
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "./service-account.json"

table = "dt-data-analytics:test.sample"
table_schema = "columna1:STRING,columna2:STRING,columna3:STRING,columna4:STRING,columna5:STRING,columna6:STRING,columna7:STRING,columna8:STRING,columna9:STRING,columna10:STRING,columna11:STRING,columna12:STRING"

def main():
    parser = argparse.ArgumentParser(description="Read Data from Cloud Storage")
    parser.version="1.0"
    parser.add_argument("--input", help="Input query")
    parser.add_argument("--out_data", help= "out data")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)


def run_pipeline(custom_args, beam_args):
    input = custom_args.input
    out_data = custom_args.out_data


    options = {"project": "dt-data-analytics",
              "runner": 'Direct',  # <------ Runtine
              "temp_location": "gs://pruebas-dt-data-analytics/tmp/",
              "region": "us-east1" }

    pipeline_options = beam.pipeline.PipelineOptions(beam_args,**options)

    #Creating pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:

        #Read data
        query_results = pipeline | "Input query" >> beam.io.ReadFromText("gs://pruebas-dt-data-analytics/data/sample.csv",skip_header_lines=1)
        #query_results | beam.Map(print)
        query_results | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                                                                      table=table,
                                                                      schema=table_schema,
                                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)



if __name__ == "__main__":
    main()