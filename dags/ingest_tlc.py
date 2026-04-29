import requests
import io
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound, Conflict

# ── Config ────────────────────────────────────────────────
GCP_PROJECT_ID  = "nyc-taxi-gcp-494519"
GCS_BUCKET_NAME = "nyc-taxi-bck"
BQ_DATASET_ID   = "raw_taxi_dataset"
GCS_LOCATION    = "US"
BQ_LOCATION     = "US"
TLC_BASE_URL    = "https://d37ci6vzurychx.cloudfront.net/trip-data"

TAXI_TYPES = ["yellow", "green"]
YEAR       = 2024
MONTH      = 1
# ─────────────────────────────────────────────────────────



def ensure_gcs_bucket_exists() -> None:
    with storage.Client(project=GCP_PROJECT_ID) as client:
        try:
            client.get_bucket(GCS_BUCKET_NAME)
            print(f"✅ GCS bucket already exists: gs://{GCS_BUCKET_NAME}")
        except NotFound:
            client.create_bucket(GCS_BUCKET_NAME, location=GCS_LOCATION)
            print(f"✅ Created GCS bucket: gs://{GCS_BUCKET_NAME}")


def ensure_bq_dataset_exists() -> None:
    with bigquery.Client(project=GCP_PROJECT_ID) as client:
        dataset_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}"
        try:
            client.get_dataset(dataset_ref)
            print(f"✅ BigQuery dataset already exists: {dataset_ref}")
        except NotFound:
            dataset          = bigquery.Dataset(dataset_ref)
            dataset.location = BQ_LOCATION
            client.create_dataset(dataset)
            print(f"✅ Created BigQuery dataset: {dataset_ref}")


def download_and_upload_to_gcs(taxi_type: str, year: int, month: int) -> None:
    """Download parquet from TLC and upload raw file to GCS."""
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url      = f"{TLC_BASE_URL}/{filename}"
    gcs_path = f"raw/{taxi_type}/{year}/{month:02d}/{filename}"

    # Check if file already exists in GCS (idempotency)
    gcs_client = storage.Client(project=GCP_PROJECT_ID)
    bucket     = gcs_client.bucket(GCS_BUCKET_NAME)
    blob       = bucket.blob(gcs_path)

    if blob.exists():
        print(f"⚠️ Already exists in GCS, skipping: gs://{GCS_BUCKET_NAME}/{gcs_path}")
        return

    print(f"Downloading {url} ...")
    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()

        print(f"Uploading to gs://{GCS_BUCKET_NAME}/{gcs_path} ...")
        blob.upload_from_file(
            io.BytesIO(response.content),
            content_type="application/octet-stream"
        )

    print(f"✅ Upload complete: gs://{GCS_BUCKET_NAME}/{gcs_path}")



def load_gcs_to_bigquery(taxi_type: str, year: int, month: int) -> None:
    """Load parquet file from GCS into BigQuery raw table."""
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    gcs_uri  = f"gs://{GCS_BUCKET_NAME}/raw/{taxi_type}/{year}/{month:02d}/{filename}"
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{taxi_type}_tripdata_{year}_{month:02d}"

    with bigquery.Client(project=GCP_PROJECT_ID) as client:

        # Check if table already exists and has rows (idempotency)
        try:
            table = client.get_table(table_id)
            if table.num_rows > 0:
                print(f"⚠️ Table already exists with {table.num_rows:,} rows, skipping: {table_id}")
                return
        except NotFound:
            pass  # table doesn't exist yet, proceed with load

        print(f"Loading {gcs_uri} → {table_id} ...")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()  # wait for completion — this is a blocking call, not a resource

        table = client.get_table(table_id)
        print(f"✅ Loaded {table.num_rows:,} rows into {table_id}")

# ── DAG definition ────────────────────────────────────────
with DAG(
    dag_id="ingest_tlc_yellow_green_2024_01",
    description="Download Yellow & Green TLC parquet files → GCS → BigQuery",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tlc", "ingestion", "raw"],
) as dag:

    # Pre-flight checks run once before any ingestion
    check_bucket = PythonOperator(
        task_id="ensure_gcs_bucket_exists",
        python_callable=ensure_gcs_bucket_exists,
    )

    check_dataset = PythonOperator(
        task_id="ensure_bq_dataset_exists",
        python_callable=ensure_bq_dataset_exists,
    )

    for taxi_type in TAXI_TYPES:

        task_upload = PythonOperator(
            task_id=f"upload_{taxi_type}_to_gcs",
            python_callable=download_and_upload_to_gcs,
            op_kwargs={"taxi_type": taxi_type, "year": YEAR, "month": MONTH},
        )

        task_load = PythonOperator(
            task_id=f"load_{taxi_type}_to_bigquery",
            python_callable=load_gcs_to_bigquery,
            op_kwargs={"taxi_type": taxi_type, "year": YEAR, "month": MONTH},
        )

        # Pre-flight checks must pass before any ingestion starts
        [check_bucket, check_dataset] >> task_upload >> task_load