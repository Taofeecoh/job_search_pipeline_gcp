from utils.module import ingest_to_gcs
from airflow.models import Variable
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import logging
from datetime import timedelta
import pendulum
import time

from google.cloud import storage
import dag_tasks.airflow_variables as airflow_variables


# Variables
airflow_vars = airflow_variables

url = "https://internships-api.p.rapidapi.com/active-jb-7d"
querystring = {
    "description_type": "text",
    "remote": "true",
    "date_filter": "2025-07-07",
    "advanced_title_filter":
    "(Data | 'Pipeline' | 'Data Engineer' | 'Analytics')"
    }

headers = {
    "x-rapidapi-key": Variable.get("INTERN_JOB_API"),
    "x-rapidapi-host": "internships-api.p.rapidapi.com"
    }

bucket_name = "general-dump"
blob = "internships_dump/"
dataset_name = "mybqdatasetid"
table_name = "mybqtableid"
file_name = (
             "de_internships_{}.csv"
             .format(time.strftime("%Y-%m-%d,%H:%M:%S"))
             )

blob_file = "{}{}".format(blob, file_name)
storage_client = storage.Client()
logging.basicConfig(
                filename="ingestion.log",
                level=logging.INFO,
                format='%(levelname)s:%(asctime)s:%(message)s'
                )


default_args = {
    "owner": "Taofeecoh",
    "start_date": pendulum.datetime(2025, 7, 5, tz="CET"),
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}


# DAG Definition
with DAG(
    dag_id="internships_dag",
    schedule_interval="0 6 * * *",
    dagrun_timeout=timedelta(minutes=1),
    default_args=default_args,
    default_view="graph",
    catchup=False, 
) as dag :
    
    extract_to_gcs = PythonOperator(
        task_id="extract_to_cloud_storage",
        python_callable=ingest_to_gcs,
        op_kwargs= {
            "url":url,
            "queryparams": querystring,
            "headers": headers,
            "blob_path": blob_file,
            "bucket": bucket_name
        }
    )
    
    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket="general-dump",
        source_objects=["internships_dump/de_internships_2025-07-09 14:37:23.csv"],
        destination_project_dataset_table=f"{dataset_name}.{table_name}",
        schema_fields=[
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "date_posted", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "date_created", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "organization", "type": "STRING", "mode": "NULLABLE"},
            {"name": "organization_url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date_validthrough", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "locations_raw", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_requirements_raw", "type": "STRING", "mode": "NULLABLE"},
            {"name": "salary_raw", "type": "STRING", "mode": "NULLABLE"},
            {"name": "employment_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "source_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "source_domain", "type": "STRING", "mode": "NULLABLE"},
            {"name": "organization_logo", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cities_derived", "type": "STRING", "mode": "NULLABLE"},
            {"name": "regions_derived", "type": "STRING", "mode": "NULLABLE"},
            {"name": "countries_derived", "type": "STRING", "mode": "NULLABLE"},
            {"name": "locations_derived", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timezones_derived", "type": "STRING", "mode": "NULLABLE"},
            {"name": "lats_derived", "type": "STRING", "mode": "NULLABLE"},
            {"name": "lngs_derived", "type": "STRING", "mode": "NULLABLE"},
            {"name": "remote_derived", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "recruiter_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "recruiter_title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "recruiter_url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_employees", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "linkedin_org_url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_size", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_slogan", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_industry", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_followers", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "linkedin_org_headquarters", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_foundeddate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_specialties", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_locations", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "linkedin_org_recruitment_agency_derived", "type": "STRING", "mode": "NULLABLE"},
            {"name": "seniority", "type": "STRING", "mode": "NULLABLE"},
            {"name": "directapply", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "linkedin_org_slug", "type": "STRING", "mode": "NULLABLE"},
            {"name": "no_jb_schema", "type": "STRING", "mode": "NULLABLE"},
            {"name": "external_apply_url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "description_text", "type": "STRING", "mode": "NULLABLE"},
        ],
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
    )

extract_to_gcs >> load_csv
