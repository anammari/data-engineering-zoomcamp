from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import io
import dask.dataframe as dd
import dask.diagnostics as diag
from dask.distributed import Client, LocalCluster

# Define BigQuery parameters
project_id = "de-zoomcamp-prj-375800"
dataset_id = "trips_data_all"
table_id = "yellow_tripdata"
table_schema = []

# Define GCS parameters
bucket_name = "dtc_data_lake_de-zoomcamp-prj-375800"
prefix = "yellow"

# Initialize BigQuery client
bq_client = bigquery.Client(project=project_id)

# Initialize GCS client
storage_client = storage.Client(project=project_id)

# Load all files into Pandas dataframes
dfs = []
for blob in storage_client.list_blobs(bucket_name, prefix=prefix):
    if blob.name.endswith(".parquet"):
        buffer = io.BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        df = pd.read_parquet(buffer)
        dfs.append(df)

# Concatenate all dataframes into one final dataframe
final_df = pd.concat(dfs, ignore_index=True, sort=True)

# Check for any data type mismatches and resolve the issues
for col in final_df.columns:
    # If data type of a column is not consistent, convert all data in that column to string
    if not final_df[col].apply(type).eq(final_df[col].apply(type).iloc[0]).all():
        final_df[col] = final_df[col].astype(str)
    # If there is a column name mismatch, rename the column to match the desired schema
    if col not in [field.name for field in table_schema]:
        final_df.rename(columns={col: col.lower()}, inplace=True)

# Get the inferred schema of the final dataframe
table_schema = []
for column in final_df.columns:
    dtype = final_df[column].dtype
    if dtype == "int64":
        bq_type = "INTEGER"
    elif dtype == "float64":
        bq_type = "FLOAT"
    elif dtype == "bool":
        bq_type = "BOOLEAN"
    else:
        bq_type = "STRING"
    field_schema = bigquery.SchemaField(column, bq_type)
    table_schema.append(field_schema)

# Create a new table with the inferred schema that matches the schema of the final Pandas dataframe
bq_client = bigquery.Client(project=project_id)
dataset_ref = bq_client.dataset(dataset_id)

table_ref = dataset_ref.table(table_id)
table = bigquery.Table(table_ref, schema=table_schema)

table = bq_client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

# Upload the data to the created table
# Create a Dask client to distribute the computation
cluster = LocalCluster(n_workers=4, threads_per_worker=1, memory_limit='4GB')
client = Client(cluster)

# Create a Dask dataframe from the Pandas dataframe
dask_df = dd.from_pandas(final_df, npartitions=4)

# Write the Dask dataframe to BigQuery table
with diag.ProgressBar(), diag.ResourceProfiler(dt=0.25) as rprof, diag.PerformanceReport() as dask_report:
    dask_df.to_gbq(destination_table=f"{dataset_id}.{table_id}",
                   project_id=project_id,
                   if_exists="replace",
                   progress_bar=True)

print("Data uploaded to BigQuery table")

# Shut down the Dask client
client.shutdown()