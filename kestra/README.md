This project is to build a data pipeline using Kestra. Downloading data from URL, saving the raw file to Google bucket and loading the values to BigQuery.

This project comes from https://github.com/DataTalksClub/data-engineering-zoomcamp

## Environment preparation
Use docker to install and intialize Kestra
```bash
docker run 
    --pull=always 
    --rm 
    -it 
    -p 8080:8080 
    --user=root
    -v /var/run/docker.sock:/var/run/docker.sock 
    -v /tmp:/tmp 
    kestra/kestra:latest server local
```

## GCP Setup
Run the following flows via Kestra UI http://localhost:8080 or run cmd to setup GCP environment for Kestra
```bash
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/gcp_kv.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/gcp_setup.yaml
```

## Data Ingestion Process
Run the following flows via Kestra UI http://localhost:8080 or run cmd to setup GCP environment for Kestra
```bash
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@flows/gcp_taxitrip_scheduled.yaml
```

## Data Navigation

- How many rows are there for the `Yellow` Taxi data for the year 2020?
```sql
SELECT count(1) FROM `secret-fountain-448312-v7.kestra_de.yellow_tripdata` 
WHERE filename LIKE '%tripdata_2020%'
```

- How many rows are there for the `Green` Taxi data for the year 2020?
```sql
SELECT count(1) FROM `secret-fountain-448312-v7.kestra_de.green_tripdata` 
WHERE filename LIKE '%tripdata_2020%'
```

- How many rows are there for the `Yellow` Taxi data for March 2021?
```sql
SELECT count(1) FROM `secret-fountain-448312-v7.kestra_de.yellow_tripdata` 
WHERE filename LIKE '%tripdata_2021-03%'
```
