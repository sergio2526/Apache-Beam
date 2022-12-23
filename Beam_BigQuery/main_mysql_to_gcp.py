import os
import argparse
import apache_beam as beam
from typing import Final
from apache_beam import PCollection
from google.cloud import secretmanager
from apache_beam.options.pipeline_options import PipelineOptions
from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL


#Credenciales y secret manager
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./service-account.json"

SECRET_USER: Final = "user"
SECRET_PASSWORD: Final = "password"
SECRET_HOST: Final = "host"
DECODE_FORMAT: Final = "UTF-8"
PROJECT_ID = "dt-data-analytics"

#Secret Manager Google Cloud
def get_name(project_id, secret_id, version):
    return f"projects/{project_id}/secrets/{secret_id}/versions/{version}"

#Obtener secreto
def get_secret(project_id, secret_id, version="latest"):
    name = get_name(project_id, secret_id, version)
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name":name})
    payload = response.payload.data.decode(DECODE_FORMAT)

    return payload



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
                                                query="SELECT * FROM PocDLP.clienteDLP;",
                                                host=get_secret(PROJECT_ID,SECRET_HOST,2), # 2 ->> Version secret
                                                database="PocDLP",
                                                user=get_secret(PROJECT_ID,SECRET_USER),
                                                password=get_secret(PROJECT_ID,SECRET_PASSWORD),
                                                port=3306,
                                                splitter=splitters.NoSplitter())  # you can select how to split query for performance
                        | "Print Results" >> beam.Map(print)
                        )

        #input_results | "Write to Local" >> beam.io.WriteToText("./out/sample_mysql_secret", file_name_suffix=".txt")



if __name__ == "__main__":
    main()