from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import ScalarFunction, udf
import os
import json
import requests
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment

# Flink SQL & Flink Java have same performance.
# Flink SQL is more readable and easy to write.

# Returns a table name which can then be used to refer to the table in the SQL query.
def create_processed_events_sink_kafka(t_env):
    table_name = "process_events_kafka"
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    sasl_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.jaas.config' = '{sasl_config}',
            'format' = 'json'
        );
        """
    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl) # Create the table inside Flink. It won't create table in Postgres.
    return table_name

# Similar to Spark UDF
# ScalarFunction is a base class for user-defined scalar functions.
# A scalar function takes zero or more scalar values (one row or one column) as input and produces a single scalar value (one row or one column) as output.
# In this case, it takes column ip_address as input and returns the location of the IP address as a column (the JSON object).
# There are other types of UDFs like TableFunction, AggregateFunction, and TableAggregateFunction.
class GetLocation(ScalarFunction):
  def eval(self, ip_address):
    url = "https://api.ip2location.io"
    response = requests.get(url, params={
        'ip': ip_address,
        'key': os.environ.get("IP_CODING_KEY")
    })

    if response.status_code != 200:
        # Return empty dict if request failed
        return json.dumps({})

    data = json.loads(response.text)

    # Extract the country and state from the response
    # This might change depending on the actual response structure
    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')
    return json.dumps({'country': country, 'state': state, 'city': city})

get_location = udf(GetLocation(), result_type=DataTypes.STRING())



def create_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"

    # WATERMARK will sequence the events in that 15 seconds window.
    # It will wait for 15 seconds to get the events in that window.
    # If the event is late, it will be discarded.
    # If the event is early, it will be stored in the buffer and will be processed when the window is complete.
    # It is useful for handling event time, late and out-of-order events in the stream.

    # connector: use the Kafka connector, we can also use other connectors like filesystem, rabbitmq, jdbc, etc.
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    print(source_ddl)
    t_env.execute_sql(source_ddl) # Execute the table environment: create the table and load the data inside Flink
    return table_name

# Flink Job starts here
def log_processing():
    print('Starting Job!')
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print('got streaming environment')
    env.enable_checkpointing(10 * 1000) # checkpoint every 10 seconds (unit of parameter is milliseconds)
    env.set_parallelism(1) # Set parallelism to 1. Max should be 2 for local development

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # Flink can run in batch mode as well.
    # settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build
    # use_blink_planner() is optional and it is optimized for batch processing.
    # The default planner is optimized for streaming processing.

    # t_env is like Dataframe in Spark
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.create_temporary_function("get_location", get_location) # Similar to registering a Spark UDF
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)
        print('loading into postgres')
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        ip,
                        event_timestamp,
                        referrer,
                        host,
                        url,
                        get_location(ip) as geodata
                    FROM {source_table}
                    """
        ).wait() # W/o wait(), the program will exit as soon it has consumed all the records from Kafka. wait() makes it run continuously.
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

# Browse to http://localhost:8081/ to see the Flink UI.
if __name__ == '__main__':
    log_processing()
