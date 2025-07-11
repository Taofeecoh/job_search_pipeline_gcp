import logging

import pandas as pd
import requests
from google.cloud import storage, bigquery


def ingest_to_gcs(endpoint: str,
                  bucket: str,
                  blob_path: str,
                  queryparams: dict | None = None,
                  headers: dict | None = None):
    
    """ 
    Function to ingest data form API to google cloud storage
    params endpoint: url to connect to 
    params bucket: name of bucket on GCP
    params blob_path: path/to/store/file
    params queryparams: querystrings to attach to endpoint's request
    params headers: header key value pair to attach to request
    return: csv data in google cloud storage
    """
    try:
        response = requests.get(endpoint, params=queryparams, headers=headers)
        if response.status_code == 200:
            response = response.json()
            response = pd.json_normalize(response)
            data = (
                storage.Client().get_bucket(bucket).blob(blob_path)
                .upload_from_string(response.to_csv(index=False),
                                    content_type="text/csv")
                )
            logging.info("Data successfully stored in cloud storage!")
            return data
        else:
            logging.info("Erorr:{}".format(response.text))
    except Exception as e:
        raise e


def create_bq_dataset(id: str,
                      location: str = "US"):
    """
    Function to create a bigquery dataset
    :params id: str, id of dataset within project
    :params location: defaults to US
    """
    client = bigquery.Client()

    # Set dataset_id to the ID of the dataset to create.
    dataset_id = "{}.{}".format(client.project, id)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = location

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    logging.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))
