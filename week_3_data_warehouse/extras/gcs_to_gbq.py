import os
from google.cloud import bigquery
from google.cloud import storage

# Set environment variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/aammari/.gc/de-zoomcamp-prj-375800-02d2ce452b16.json'
project_id = 'de-zoomcamp-prj-375800'

# Initialize BigQuery client
bq_client = bigquery.Client(project=project_id)

# Initialize GCS client
storage_client = storage.Client(project=project_id)

# Set table and bucket details
bucket_name = 'dtc_data_lake_de-zoomcamp-prj-375800'
table_name = 'green_tripdata'
dataset_name = 'staging'

# Set the table schema
schema = [
    bigquery.SchemaField('VendorID', 'INTEGER'),
    bigquery.SchemaField('pickup_datetime', 'TIMESTAMP'),
    bigquery.SchemaField('dropoff_datetime', 'TIMESTAMP'),
    bigquery.SchemaField('passenger_count', 'INTEGER'),
    bigquery.SchemaField('trip_distance', 'FLOAT'),
    bigquery.SchemaField('pickup_longitude', 'FLOAT'),
    bigquery.SchemaField('pickup_latitude', 'FLOAT'),
    bigquery.SchemaField('RateCodeID', 'INTEGER'),
    bigquery.SchemaField('store_and_fwd_flag', 'STRING'),
    bigquery.SchemaField('dropoff_longitude', 'FLOAT'),
    bigquery.SchemaField('dropoff_latitude', 'FLOAT'),
    bigquery.SchemaField('payment_type', 'INTEGER'),
    bigquery.SchemaField('fare_amount', 'FLOAT'),
    bigquery.SchemaField('extra', 'FLOAT'),
    bigquery.SchemaField('mta_tax', 'FLOAT'),
    bigquery.SchemaField('tip_amount', 'FLOAT'),
    bigquery.SchemaField('tolls_amount', 'FLOAT'),
    bigquery.SchemaField('improvement_surcharge', 'FLOAT'),
    bigquery.SchemaField('total_amount', 'FLOAT'),
    bigquery.SchemaField('PULocationID', 'INTEGER'),
    bigquery.SchemaField('DOLocationID', 'INTEGER')
]

# Define the GCS bucket and path
bucket = storage_client.get_bucket(bucket_name)
blobs = bucket.list_blobs(prefix='green')

# Load each file into BigQuery
for blob in blobs:
    if 'parquet' in blob.name:
        # Set the table ID and load job configuration
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,
        )

        # Load the data into BigQuery
        print(f"Loading {blob.name} into BigQuery...")
        uri = f"gs://{bucket_name}/{blob.name}"
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)

        # Wait for the job to complete
        load_job.result()

        # Get the newly created table and check its schema
        table_ref = bq_client.dataset(dataset_name).table(table_name)
        table = bq_client.get_table(table_ref)
        print(f"Table {table.project}.{table.dataset_id}.{table.table_id} successfully loaded.")

# Create the table if it doesn't exist
try:
    bq_client.get_table(table_ref)
except Exception as e:
    print(f"Table not found: {e}. Creating table {table_name}...")
    table = bigquery.Table(table_ref, schema=schema)
    table = bq_client.create_table(table)
    print(f"Table {table.project}.{table.dataset_id}.{table.table_id} successfully created.")    
