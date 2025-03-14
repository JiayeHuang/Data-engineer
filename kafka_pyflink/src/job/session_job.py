from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.window import Session
from pyflink.table.expressions import col, lit


topic_name = 'green-trips'


def create_events_source_kafka(t_env):
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss"    
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = '{topic_name}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def create_events_aggregated_sink(t_env):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP(3),
            PULocationID INTEGER,
            DOLocationID INTEGER,
            session_duration BIGINT,
            PRIMARY KEY (event_hour, PULocationID, DOLocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        t_env.from_path(source_table) \
            .window(
                Session.with_gap(lit(5).minutes).on(col("dropoff_timestamp")).alias("w")
            ).group_by(
                col("w"), col("PULocationID"), col("DOLocationID")
            ).select(
                col("w").start.alias("event_hour"),
                col("PULocationID"),
                col("DOLocationID"),
                (col("w").end - col("w").start).alias("session_duration")
            ).execute_insert(aggregated_table).wait()
                # col("PULocationID").count.alias("num_hits")
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
