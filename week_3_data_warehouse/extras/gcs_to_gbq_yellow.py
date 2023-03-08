from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import io
import dask.dataframe as dd
import dask.diagnostics as diag
from dask.distributed import Client, LocalCluster
import multiprocessing

def worker():
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

    # Create a Dask client to distribute the computation
    cluster = LocalCluster(n_workers=8, threads_per_worker=1, memory_limit='16GB', dashboard_address=':8787')
    client = Client(cluster)

    # Create a Dask dataframe from Parquet files stored in GCS
    dask_df = dd.read_parquet(f"gcs://{bucket_name}/{prefix}*.parquet")

    # Check for any data type mismatches and resolve the issues
    for col in dask_df.columns:
        # If data type of a column is not consistent, convert all data in that column to string
        if not dask_df[col].apply(type).eq(dask_df[col].apply(type).iloc[0]).all():
            dask_df[col] = dask_df[col].astype(str)
        # If there is a column name mismatch, rename the column to match the desired schema
        if col not in [field.name for field in table_schema]:
            dask_df.rename(columns={col: col.lower()}, inplace=True)

    # Get the inferred schema of the final dataframe
    table_schema = []
    for column, dtype in dask_df.dtypes.iteritems():
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

    # Write the Dask dataframe to BigQuery table
    dask_df.to_gbq(destination_table=f"{dataset_id}.{table_id}",
                project_id=project_id,
                if_exists="replace",
                progress_bar=True)

    print("Data uploaded to BigQuery table")

    # Shut down the Dask client
    client.shutdown()

if __name__ == '__main__':
    multiprocessing.Process(target=worker).start()