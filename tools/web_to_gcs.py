import os, argparse
import requests
import pandas as pd
from google.cloud import storage

"""
Ref: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/extras/web_to_gcs.py
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""


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


def web_to_gcs(year, service, bucket):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip')
        parquet_file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(parquet_file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(bucket, f"{service}/{parquet_file_name}", parquet_file_name)
        print(f"GCS: {service}/{file_name}")

        os.remove(file_name)
        os.remove(parquet_file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download gz/csv files from provided url, load the data to pstgresql database')
    parser.add_argument('--bucket', '-b', help='bucket to upload files to')
    parser.add_argument('--service', '-s', help='services = ["fhv","green","yellow"]')
    parser.add_argument('--year', '-y', help='year of data')
       
    args = parser.parse_args()

    init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
    BUCKET = os.environ.get("GCP_GCS_BUCKET", args.bucket)
    
    web_to_gcs(args.year, args.service, BUCKET)
