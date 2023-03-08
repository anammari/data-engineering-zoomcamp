import pandas as pd
import requests
import os
from google.cloud import storage

# Define parameters
base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
years = [2019, 2020]
months = ['{:02d}'.format(i) for i in range(1, 13)]
bucket_name = 'dtc_data_lake_de-zoomcamp-prj-375800'
folder_name = 'yellow'
project_id = 'de-zoomcamp-prj-375800'
key_path = 'C:/Users/aammari/.gc/de-zoomcamp-prj-375800-02d2ce452b16.json'

# Create a client to access Cloud Storage
storage_client = storage.Client.from_service_account_json(key_path)

# Create the bucket if it doesn't exist
bucket = storage_client.bucket(bucket_name)
if not bucket.exists():
    bucket.create(location='us')

# Loop over years and months to download and process files
for year in years:
    for month in months:
        # Download the file
        file_name = f'yellow_tripdata_{year}-{month}.csv.gz'
        file_url = f'{base_url}{file_name}'
        response = requests.get(file_url)
        
        # Save the file to disk
        file_path = file_name.replace('.csv.gz', '.parquet')
        with open(file_name, 'wb') as f:
            f.write(response.content)

        # Load the data from the CSV file and convert it to parquet
        df = pd.read_csv(file_name, dtype={'VendorID': 'str', 'store_and_fwd_flag': 'str'}, low_memory=False)
        df.to_parquet(file_path)
        
        # Upload the parquet file to Cloud Storage
        blob = bucket.blob(f'{folder_name}/{file_path}')
        blob.upload_from_filename(file_path)

        # Delete the local file
        os.remove(file_path)
        os.remove(file_name)
        
        print(f'{file_name} processed and uploaded to Cloud Storage.')
