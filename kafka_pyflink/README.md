## Environment preparation
To install Red Panda, Flink Job Manager, Flink Task Manager and Psotgres in the docker

The docker files are copied from: https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/06-streaming/pyflink

```bash
docker-compose up --remove-orphans --build
```

Use pgcli to connect to postgres
```bash
pgcli -h localhost -p 5432 -u postgres -d postgres
```

Run query to create the table in postgres
```bash
CREATE TABLE processed_events (
    lpep_pickup_datetime VARCHAR,
    lpep_dropoff_datetime VARCHAR,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    session_duration BIGINT
);
```

## Container playground

Optional: Edit user for the folders mounted as volume of container
```bash
sudo chown -R $USER:$USER ./src/
sudo chown -R $USER:$USER ./keys/
```

Run the container in an interative way
```bash
docker exec -it redpanda-1 bash
```

Check Redpanda version
```bash
rpk --version
```

Create a topic with name "green-trips"
```bash
rpk topic create green-trips
```

## Produce messages to topic

```bash
python green_trips_producer.py
```

## Ingest data from the topic

Run green_trips_job to ingest the raw data and save them to postgres table "processed_events"
```bash
docker-compose exec jobmanager ./bin/flink run -py /opt/src/job/green_trips_job.py -d
```

Run session_job to conduct group aggregation
```bash
docker-compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job.py -d
```
