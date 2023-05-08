import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

#Service
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "../service-account.json"


options = PipelineOptions.from_dictionary({
    "project": "dt-data-analytics",
    "runner": "Dataflow",
    "temp_location": "gs://pruebas-dt-data-analytics/tmp/",
    "region": "us-east1"
})

p = beam.Pipeline(options=options)

(p
 | "Create" >> beam.Create(["Hola mundo"])
 | "Print" >> beam.Map(print))

p.run()