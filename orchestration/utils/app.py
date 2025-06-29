import logging
import os
import time

from dotenv import load_dotenv
from google.cloud import storage

from module import ingest_to_gcs

load_dotenv()
url = "https://internships-api.p.rapidapi.com/active-jb-7d"

querystring = {
    "description_type": "text",
    "remote": "true",
    "date_filter": "2025-06-26",
    "advanced_title_filter":
    "(Data | 'Pipeline' | 'Data Engineer' | 'Analytics')"
    }

headers = {
    "x-rapidapi-key": os.getenv("INTERN_JOB_API"),
    "x-rapidapi-host": "internships-api.p.rapidapi.com"
    }

bucket_name = "all-jobs-lake"
file_name = (
             "de_internships_{}.csv"
             .format(time.strftime("%Y-%m-%d %H:%M:%S"))
             )
blob = "internships_dump/"
blob_file = "{}{}".format(blob, file_name)
storage_client = storage.Client()
logging.basicConfig(
                filename="ingestion.log",
                level=logging.INFO,
                format='%(levelname)s:%(message)s:%(asctime)s'
                )

data = ingest_to_gcs(
    url,
    queryparams=querystring,
    headers=headers,
    blob_path=blob_file,
    bucket=bucket_name
    )

print(data)
