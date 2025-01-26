# Docker & SQL

This project is to build a data pipeline using docker, sql and implement cloud environment using Terraform. The data pipeline will digest data from a given url (support csv/gz file), load it to postgresql and allow query via web UI using pgAdmin.

This project comes from https://github.com/DataTalksClub/data-engineering-zoomcamp

## Environment preparation
docker first run, to run docker container in an interactive way. Check the version of pip.
```bash
docker run -it python:3.12.8 bash
pip --version
```

Install and run postgres pgAdmin images
```bash
docker-compose up -d
```

## Access the container
Access using pgcli from server
```bash
pgcli -h localhost -p 5433 -u postgres -d ny_taxi
```
Access using pgAdmin via host/port: postgres:5432

## Data Ingestion
Download files from url and ingest the data into postgresql
- Directly use python
```bash
cd ./Scritps
```
```bash
python load_data.py \
--url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv \
--user postgres \
--password postgres \
--host localhost \
--port 5433 \
--db ny_taxi
```
```bash
python load_data.py \
--url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz \
--user postgres \
--password postgres \
--host localhost \
--port 5433 \
--db ny_taxi
```

- Ingest using docker
```bash
cd ./Scritps
docker build -t load_taxi_data:v001 .
```
```bash
docker run -it \
    --network=docker_sql_default \
    load_taxi_data:v001 \
        --user=postgres \
        --password=postgres \
        --host=postgres \
        --port=5432 \
        --db=ny_taxi \
        --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```
```bash
docker run -it \
    --network=docker_sql_default \
    load_taxi_data:v001 \
        --user=postgres \
        --password=postgres \
        --host=postgres \
        --port=5432 \
        --db=ny_taxi \
        --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```
## SQl queries
## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
```sql
SELECT count(1) FROM public."green_tripdata_2019-10"
where lpep_dropoff_datetime >= '2019-10-01' and lpep_dropoff_datetime < '2019-11-01'
    and lpep_pickup_datetime >= '2019-10-01' and lpep_pickup_datetime < '2019-11-01'
    and trip_distance<=1;
```
2. In between 1 (exclusive) and 3 miles (inclusive),
```sql
SELECT count(1) FROM public."green_tripdata_2019-10"
where lpep_dropoff_datetime >= '2019-10-01' and lpep_dropoff_datetime < '2019-11-01'
    and lpep_pickup_datetime >= '2019-10-01' and lpep_pickup_datetime < '2019-11-01'
    and trip_distance>1 and trip_distance<=3;
```
3. In between 3 (exclusive) and 7 miles (inclusive),
```sql
SELECT count(1) FROM public."green_tripdata_2019-10"
where lpep_dropoff_datetime >= '2019-10-01' and lpep_dropoff_datetime < '2019-11-01'
    and lpep_pickup_datetime >= '2019-10-01' and lpep_pickup_datetime < '2019-11-01'
    and trip_distance>3 and trip_distance<=7;
```
4. In between 7 (exclusive) and 10 miles (inclusive),
```sql
SELECT count(1) FROM public."green_tripdata_2019-10"
where lpep_dropoff_datetime >= '2019-10-01' and lpep_dropoff_datetime < '2019-11-01'
    and lpep_pickup_datetime >= '2019-10-01' and lpep_pickup_datetime < '2019-11-01'
    and trip_distance>7 and trip_distance<=10;
```
5. Over 10 miles 
```sql
SELECT count(1) FROM public."green_tripdata_2019-10"
where lpep_dropoff_datetime >= '2019-10-01' and lpep_dropoff_datetime < '2019-11-01'
    and lpep_pickup_datetime >= '2019-10-01' and lpep_pickup_datetime < '2019-11-01'
    and trip_distance>10;
```

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

```sql
SELECT CAST(lpep_pickup_datetime AS DATE)
, max(trip_distance) AS max_distance 
FROM public."green_tripdata_2019-10"
group by CAST(lpep_pickup_datetime AS DATE)
order by max_distance DESC
limit 3;
```

## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
 
```sql
SELECT t."Zone" as region
FROM public."green_tripdata_2019-10" g
    left join public."taxi_zone_lookup" t
    on g."PULocationID" = t."LocationID"
WHERE CAST(g.lpep_pickup_datetime AS DATE) = '2019-10-18'
group by "PULocationID", t."Zone"
HAVING SUM(total_amount)>13000;
```

## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone
named "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

```sql
SELECT tdof."Zone" as region
,max(pu.tip_amount) as max_tip
FROM (public."green_tripdata_2019-10" g
        left join public."taxi_zone_lookup" t
        on g."PULocationID" = t."LocationID") pu
    left join public."taxi_zone_lookup" tdof
    on pu."DOLocationID" = tdof."LocationID"
WHERE pu.lpep_pickup_datetime >= '2019-10-01' AND pu.lpep_pickup_datetime < '2019-11-01'
AND pu."Zone" = 'East Harlem North'
group by tdof."Zone"
order by max_tip DESC
limit 3;
```

## Terraform
## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

```bash
terraform init
terraform plan
terraform apply -auto-approve
terraform destroy
```