import argparse
from google.cloud import bigquery

"""
Ref: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#python
"""

def main(param):
    client = bigquery.Client()
    table_id = f"{param.project_id}.{param.dataset}.{param.table}"

    # Define the schema explicitly
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        # write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,        
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(param.uri, table_id, job_config=job_config)
    print(f"Starting job {load_job.job_id}")

    load_job.result()  # Wait for the job to complete
    print("Load job finished.")

    # Optionally, confirm the row count
    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download gz/csv files from provided url, load the data to pstgresql database')
    parser.add_argument('--project_id', '-p', help='project id')
    parser.add_argument('--dataset', '-d', help='bigquery dataset')
    parser.add_argument('--table', '-t', help='bigquery table')
    parser.add_argument('--uri', '-u', help='gcs bucket uri')
       
    args = parser.parse_args()

    main(args)
