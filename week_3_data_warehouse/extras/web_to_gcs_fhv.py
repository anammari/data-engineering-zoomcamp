import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage
import gzip

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/'
# switch out the bucketname
#BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_de-zoomcamp-prj-375800")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'

        # download it using requests via a pandas df
        request_url = init_url + file_name
        r = requests.get(request_url)
        
        # Decompress the gzipped file
        with open(io.BytesIO(r.content)) as f:
            csv_data = f.read().decode('utf-8')

        # Convert CSV data to pandas DataFrame
        df = pd.read_csv(io.StringIO(csv_data))
        print(f"Local: {file_name}")

        # Convert to parquet and write to file
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


web_to_gcs('2019', 'fhv')
#web_to_gcs('2020', 'green')
