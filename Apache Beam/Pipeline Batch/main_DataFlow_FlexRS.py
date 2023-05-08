import os
import argparse
import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions

#Service
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "../service-account.json"


def main():
    parser = argparse.ArgumentParser(description="Read Data from BigQuery")
    parser.version="1.0"
    parser.add_argument("--query", help="Input query")
    parser.add_argument("--out_data", help= "out data")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)


def run_pipeline(custom_args, beam_args):
    query = custom_args.query
    out_data = custom_args.out_data

    options = {"project": "dt-data-analytics",
               "runner": "DataflowRunner",
               "temp_location": "gs://pruebas-dt-data-analytics/tmp/",
               "region": "us-central1",
               "flexrs_goal": "COST_OPTIMIZED" 
              # "worker_region": "us-central1",
              # "max_num_workers": 100,
              # "autoscaling_algorithm": "THROUGHPUT_BASED"
              
              }

    pipeline_options = beam.pipeline.PipelineOptions(beam_args, **options)

    #Creating pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:

        #Read data
        query_results = pipeline | "Input query" >> beam.io.ReadFromBigQuery(query = query,
                                                                             use_standard_sql=True)

        query_results | "Write to Cloud Storage" >> beam.io.WriteToText(out_data, file_name_suffix=".txt")

if __name__ == "__main__":
    main()