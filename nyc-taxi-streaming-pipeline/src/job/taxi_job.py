from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_taxi_events_sink_postgres(t_env):
    table_name = 'taxi_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE
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


def create_events_source_kafka(t_env):
    table_name = "taxi_events_source"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            Pickup_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, '{pattern}'),
            WATERMARK FOR Pickup_timestamp AS Pickup_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'taxi-data',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'properties.group.id' = 'flink-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(1000)
    # env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_taxi_events_sink_postgres(t_env)

        t_env.execute_sql(
            f"""
            INSERT INTO {postgres_sink}
            SELECT
                lpep_pickup_datetime,
                lpep_dropoff_datetime,
                PULocationID,
                DOLocationID,
                passenger_count,
                trip_distance,
                tip_amount
            FROM {source_table}
            """
        ).wait()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
