import io
import itertools
import requests
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound

# ── Config ────────────────────────────────────────────────
GCP_PROJECT_ID  = "nyc-taxi-gcp-494519"
GCS_BUCKET_NAME = "nyc-taxi-bck"
BQ_DATASET_ID   = "raw_taxi_dataset"
GCS_LOCATION    = "US"
BQ_LOCATION     = "US"
TLC_BASE_URL    = "https://d37ci6vzurychx.cloudfront.net/trip-data"

TAXI_TYPES  = ["yellow", "green"]
START_YEAR  = 2024
START_MONTH = 1

PARTITION_FIELD = {
    "yellow": "tpep_pickup_datetime",
    "green":  "lpep_pickup_datetime",
}
# ─────────────────────────────────────────────────────────


def _periods():
    """Yield (year, month) tuples from START up to and including the current month."""
    now = datetime.now()
    for year, month in itertools.product(range(START_YEAR, now.year + 1), range(1, 13)):
        if (year, month) < (START_YEAR, START_MONTH):
            continue
        if (year, month) > (now.year, now.month):
            break
        yield year, month


def _sync_schema(client: bigquery.Client, source_table_id: str, target_table_id: str) -> None:
    """Add any columns present in source but missing from target. No-op if schemas already match."""
    source_schema = client.get_table(source_table_id).schema
    target_table  = client.get_table(target_table_id)
    target_names  = {f.name for f in target_table.schema}

    new_fields = [f for f in source_schema if f.name not in target_names]
    if new_fields:
        target_table.schema = list(target_table.schema) + new_fields
        client.update_table(target_table, ["schema"])
        print(f"🔄 Added {len(new_fields)} new column(s): {[f.name for f in new_fields]}")
    else:
        print("✅ Schema already up to date, no new columns to add.")


# ── Pre-flight checks ─────────────────────────────────────

def ensure_gcs_bucket_exists() -> None:
    client = storage.Client(project=GCP_PROJECT_ID)
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


# ── Core ingestion functions ──────────────────────────────

def download_and_upload_to_gcs(taxi_type: str, year: int, month: int) -> None:
    """Download parquet from TLC and upload raw file to GCS. Idempotent — skips if already exists."""
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url      = f"{TLC_BASE_URL}/{filename}"
    gcs_path = f"raw/{taxi_type}/{year}/{month:02d}/{filename}"

    gcs_client = storage.Client(project=GCP_PROJECT_ID)
    bucket     = gcs_client.bucket(GCS_BUCKET_NAME)
    blob       = bucket.blob(gcs_path)

    if blob.exists():
        print(f"⚠️  Already exists in GCS, skipping: gs://{GCS_BUCKET_NAME}/{gcs_path}")
        return

    print(f"Downloading {url} ...")
    with requests.get(url, stream=True, timeout=300) as response:
        if response.status_code == 404:
            print(f"⏭️  File not yet published by TLC, skipping: {url}")
            return

        response.raise_for_status()

        print(f"Uploading to gs://{GCS_BUCKET_NAME}/{gcs_path} ...")
        blob.upload_from_file(
            io.BytesIO(response.content),
            content_type="application/octet-stream",
        )

    print(f"✅ Upload complete: gs://{GCS_BUCKET_NAME}/{gcs_path}")


def load_gcs_to_bigquery(taxi_type: str, year: int, month: int) -> None:
    """
    Load one parquet file from GCS into the main BQ table for this taxi type.

    Strategy:
      1. Idempotency check  — skip if _source_file already present in main table.
      2. Load parquet → temp table using autodetect (handles any schema evolution).
      3. First load: create main table from temp schema + partitioning + clustering.
         Subsequent loads: sync any new columns from temp → main via _sync_schema.
      4. INSERT INTO main SELECT *, filename AS _source_file FROM temp.
      5. Delete temp table.

    Schema evolves automatically — no hardcoded field lists anywhere.
    """
    filename      = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    gcs_uri       = f"gs://{GCS_BUCKET_NAME}/raw/{taxi_type}/{year}/{month:02d}/{filename}"
    main_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.raw_{taxi_type}_tripdata"
    temp_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{taxi_type}_tripdata_tmp_{year}_{month:02d}"
    partition_col = PARTITION_FIELD[taxi_type]

    with bigquery.Client(project=GCP_PROJECT_ID) as client:

        # ── Step 1: Idempotency check ──────────────────────────────────────
        try:
            existing = client.query(f"""
                SELECT COUNT(1) AS cnt
                FROM `{main_table_id}`
                WHERE _source_file = '{filename}'
            """).result()

            if next(existing).cnt > 0:
                print(f"⚠️  Already loaded, skipping: {filename}")
                return
        except NotFound:
            pass  # Main table doesn't exist yet — first ever load, proceed.

        # ── Step 2: Load parquet → temp with autodetect ────────────────────
        # Autodetect handles any schema — 2024 files without cbd_congestion_fee,
        # 2025 files with it, or any future columns TLC may add.
        print(f"Loading {gcs_uri} → temp table {temp_table_id} ...")
        load_job = client.load_table_from_uri(
            gcs_uri,
            temp_table_id,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True,
            ),
        )
        load_job.result()

        # ── Step 3: Create or evolve main table ────────────────────────────
        try:
            client.get_table(main_table_id)
            # Table exists — sync any new columns from this file's schema.
            print(f"Syncing schema from temp → {main_table_id} ...")
            _sync_schema(client, temp_table_id, main_table_id)

        except NotFound:
            # First ever load — create partitioned + clustered main table.
            temp_schema = client.get_table(temp_table_id).schema
            full_schema = list(temp_schema) + [
                bigquery.SchemaField(
                    "_source_file", "STRING",
                    description="Source parquet filename, e.g. yellow_tripdata_2024-01.parquet"
                )
            ]
            main_table = bigquery.Table(main_table_id, schema=full_schema)
            main_table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field=partition_col,
            )
            main_table.clustering_fields        = ["_source_file"]
            main_table.require_partition_filter = False

            client.create_table(main_table)
            print(
                f"✅ Created {main_table_id} | "
                f"partitioned by {partition_col} (MONTH) | "
                f"clustered by _source_file"
            )

        # ── Step 4: INSERT into main table, injecting _source_file ─────────
        # INSERT with explicit column mapping ────────────────────
        main_schema   = client.get_table(main_table_id).schema
        temp_schema   = client.get_table(temp_table_id).schema
        temp_col_names = {f.name for f in temp_schema}

        # All data columns from main (excluding _source_file)
        main_data_cols = [f.name for f in main_schema if f.name != "_source_file"]

        # SELECT each column by name; NULL for columns not in this file (e.g. 2024 file missing cbd_congestion_fee)
        select_parts = [
            f"`{col}`" if col in temp_col_names else f"NULL AS `{col}`"
            for col in main_data_cols
        ]

        insert_cols   = ", ".join([f"`{c}`" for c in main_data_cols] + ["`_source_file`"])
        select_clause = ", ".join(select_parts)

        print(f"Inserting {filename} → {main_table_id} ...")
        client.query(f"""
            INSERT INTO `{main_table_id}` ({insert_cols})
            SELECT {select_clause}, '{filename}'
            FROM `{temp_table_id}`
        """).result()

        # ── Step 5: Delete temp table ──────────────────────────────────────
        client.delete_table(temp_table_id)
        print(f"🗑️  Temp table deleted: {temp_table_id}")


# ── DAG definition ────────────────────────────────────────
with DAG(
    dag_id="ingest_tlc_yellow_green_all",
    description=(
        "Ingest Yellow & Green TLC parquet files → GCS → BigQuery "
        "for all months from 2024-01 through the current month. "
        "One partitioned+clustered BQ table per taxi type."
    ),
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tlc", "ingestion", "raw"],
) as dag:

    check_bucket = PythonOperator(
        task_id="ensure_gcs_bucket_exists",
        python_callable=ensure_gcs_bucket_exists,
    )

    check_dataset = PythonOperator(
        task_id="ensure_bq_dataset_exists",
        python_callable=ensure_bq_dataset_exists,
    )

    for year, month in _periods():
        for taxi_type in TAXI_TYPES:

            task_upload = PythonOperator(
                task_id=f"upload_{taxi_type}_{year}_{month:02d}_to_gcs",
                python_callable=download_and_upload_to_gcs,
                op_kwargs={"taxi_type": taxi_type, "year": year, "month": month},
            )

            task_load = PythonOperator(
                task_id=f"load_{taxi_type}_{year}_{month:02d}_to_bigquery",
                python_callable=load_gcs_to_bigquery,
                op_kwargs={"taxi_type": taxi_type, "year": year, "month": month},
            )

            # Upload needs bucket ready; load needs dataset ready + file uploaded.
            # Table creation and schema evolution are handled inside load itself.
            [check_bucket, check_dataset] >> task_upload >> task_load