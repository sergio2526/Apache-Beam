import os
import argparse
import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions
from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL

def main():
    parser = argparse.ArgumentParser(description="Read Data Mysql Instance Google Cloud")
    parser.version="1.0"

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)


def run_pipeline(custom_args, beam_args):

    options = {"runner": 'Direct'}
    pipeline_options = beam.pipeline.PipelineOptions(beam_args,**options)

    #Creating pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:

        #Read data
        input_results = (pipeline
                        | "Read from mysql" >> ReadFromMySQL(
                                                query="",
                                                host="",
                                                database="",
                                                user="",
                                                password='',
                                                port="",
                                                splitter=splitters.NoSplitter())  # you can select how to split query for performance
                        #| "Print Results" >> beam.Map(print)
                        )

        input_results | "Write to Local" >> beam.io.WriteToText("./out/sample_mysql", file_name_suffix=".txt")



if __name__ == "__main__":
    main()