import os
import argparse
import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions

def main():
    parser = argparse.ArgumentParser(description="Read Data Local")
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
                        | "Import Data" >> beam.io.ReadFromText("../data/sample.csv", skip_header_lines=1)
                        | "Split by comma" >> beam.Map(lambda record: record.split(','))
                        | beam.Map(lambda record: int(record[2])*10) # seleccionar la columna posicion 2 y multiplicar por 10
                        )

        input_results | "Write to Local" >> beam.io.WriteToText("../out/sample1", file_name_suffix=".txt")



if __name__ == "__main__":
    main()