import logging

import pandas as pd
import requests
from google.cloud import storage


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
                storage.Client.get_bucket(bucket).blob(blob_path)
                .upload_from_string(response.to_csv(index=False),
                                    content_type="text/csv")
                )
            logging.info("Data successfully stored in cloud storage!")
            return data
        else:
            logging.info("Erorr:{}".format(response.text))
    except Exception as e:
        raise e
