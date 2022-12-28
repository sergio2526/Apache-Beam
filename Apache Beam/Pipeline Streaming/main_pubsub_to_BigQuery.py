import os
import json
import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions


#Credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../service-account.json"

table = "dt-data-analytics:test.table_pubsub"
table_schema = "message:STRING, number:INTEGER"


def parse_json(element):
    row = json.loads(element.decode('utf-8'))
    return row


def run_pipeline():

    options = {"project": "dt-data-analytics",
              "runner": 'DataFlow',  # <------ Runtine
              "temp_location": "gs://pruebas-dt-data-analytics/tmp/",
              "region": "us-east1",
              "streaming": True }

    #options = {"runner":"Direct",
    #            "streaming": True}

    pipeline_options = beam.pipeline.PipelineOptions(**options)

    #Creating pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:

        #Lee mensajes de un tópico de Pub/Sub y los envía a la pipeline.
        messages = (pipeline | 'Lee mensajes de Pub/Sub' >> ReadFromPubSub(topic='projects/dt-data-analytics/topics/MyTopicTest')
                             | 'ParseJson' >> beam.Map(parse_json)
                             ) 

        # Aplica una ventana de tiempo fijo de 5 segundos a cada elemento de la pipeline.
        (messages | 'Añade ventanas de tiempo' >> beam.WindowInto(FixedWindows(5))
                  | 'WriteAggToBQ' >> beam.io.WriteToBigQuery(
                    table,
                    schema=table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                    ))


if __name__ == "__main__":
    run_pipeline()