import logging
import os
import time

from dotenv import load_dotenv
from google.cloud import bigquery
from module import ingest_to_gcs, create_bq_dataset

# load_dotenv()
# url = "https://internships-api.p.rapidapi.com/active-jb-7d"

# querystring = {
#     "description_type": "text",
#     "remote": "true",
#     "date_filter": "2025-06-26",
#     "advanced_title_filter":
#     "(Data | 'Pipeline' | 'Data Engineer' | 'Analytics')"
#     }

# headers = {
#     "x-rapidapi-key": os.getenv("INTERN_JOB_API"),
#     "x-rapidapi-host": "internships-api.p.rapidapi.com"
#     }

# bucket_name = "general-dump"
# file_name = (
#              "de_internships_{}.csv"
#              .format(time.strftime("%Y-%m-%d %H:%M:%S"))
#              )
# blob = "internships_dump/"
# blob_file = "{}{}".format(blob, file_name)

# logging.basicConfig(
#                 filename="ingestion.log",
#                 level=logging.INFO,
#                 format='%(levelname)s:%(message)s:%(asctime)s'
#                 )


# # Ingest to cloud storage
# data = ingest_to_gcs(
#     url,
#     queryparams=querystring,
#     headers=headers,
#     blob_path=blob_file,
#     bucket=bucket_name
#     )

# Create dataset object
id_of_dataset = "my_test_dataset"
# dataset_object = create_bq_dataset(id=id_of_dataset, location="US")


# Create test table
client = bigquery.Client()
table_id = "{}.{}.test_table_name".format
(client.project, bigquery.Dataset(f"{client.project}.{id_of_dataset}").dataset_id)

job_config = bigquery.LoadJobConfig(
    # schema=[
    #     bigquery.SchemaField("name", "STRING"),
    #     bigquery.SchemaField("post_abbr", "STRING"),
    # ],
    schema =[
        bigquery.SchemaField(name="id", field_type="STRING", mode= "REQUIRED"),
        bigquery.SchemaField(name="date_posted", field_type="DATETIME", mode="NULLABLE"),
        bigquery.SchemaField(name="date_created", field_type="DATETIME", mode="NULLABLE"),
        bigquery.SchemaField(name="title", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="organization", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="organization_url", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="date_validthrough", field_type="DATETIME", mode="NULLABLE"),
        bigquery.SchemaField(name="locations_raw", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="location_type", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="location_requirements_raw", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="salary_raw", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="employment_type", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="url", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="source_type", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="source", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="source_domain", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="organization_logo", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="cities_derived", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="regions_derived", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="countries_derived", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="locations_derived", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="timezones_derived", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="lats_derived", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="lngs_derived", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="remote_derived", field_type="BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField(name="recruiter_name", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="recruiter_title", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="recruiter_url", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_employees", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_url", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_size", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_slogan", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_industry", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_followers", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_headquarters", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_type", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_foundeddate", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_specialties", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_locations", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_description", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_recruitment_agency_derived", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="seniority", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="directapply", field_type="BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField(name="linkedin_org_slug", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="no_jb_schema", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="external_apply_url", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="description_text", field_type="STRING", mode="NULLABLE"),
        ],
    
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://general-dump/internships_dump/de_internships_2025-07-09 14:37:23.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))
