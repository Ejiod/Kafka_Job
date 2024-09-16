from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import os

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Create table environment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(environment_settings=env_settings)

# Add Kafka connector JAR, PostgreSQL JDBC driver, and Flink JDBC connector
t_env.get_config().get_configuration().set_string(
    "pipeline.jars", 
    f"file://{os.path.abspath('flink-sql-connector-kafka-3.2.0-1.19.jar')};"
    f"file://{os.path.abspath('postgresql-42.7.4.jar')};"
    f"file://{os.path.abspath('flink-connector-jdbc-3.2.0-1.19.jar')}"
)

# Define source tables for each topic
for topic in ['BTC', 'SOL', 'BNB']:
    source_ddl = f"""
        CREATE TABLE kafka_source_{topic.lower()} (
            symbol STRING,
            price DOUBLE,
            event_time_ms BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink-consumer-group',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'  -- Start from the latest offset to process new messages
        )
    """
    t_env.execute_sql(source_ddl)

# Define PostgreSQL sink table
postgres_sink_ddl = """
    CREATE TABLE postgres_sink (
        symbol STRING,
        price DOUBLE,
        event_time TIMESTAMP(3),
        topic STRING,
        PRIMARY KEY (symbol, topic) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/milk',
        'table-name' = 'machine.crypto_prices',
        'username' = 'mac',
        'password' = '1234'
    )
"""
t_env.execute_sql(postgres_sink_ddl)

# Define a processing query that combines data from all topics and inserts into PostgreSQL
insert_stmt = """
    INSERT INTO postgres_sink
    SELECT symbol, price, TO_TIMESTAMP(FROM_UNIXTIME(event_time_ms / 1000)) AS event_time, 'BTC' AS topic
    FROM kafka_source_btc
    WHERE event_time_ms > 1000000000000  -- Adjust filter for event_time_ms in milliseconds
    UNION ALL
    SELECT symbol, price, TO_TIMESTAMP(FROM_UNIXTIME(event_time_ms / 1000)) AS event_time, 'SOL' AS topic
    FROM kafka_source_sol
    WHERE event_time_ms > 1000000000000
    UNION ALL
    SELECT symbol, price, TO_TIMESTAMP(FROM_UNIXTIME(event_time_ms / 1000)) AS event_time, 'BNB' AS topic
    FROM kafka_source_bnb
    WHERE event_time_ms > 1000000000000
"""

# Execute the query and send results to the PostgreSQL sink
t_env.execute_sql(insert_stmt).wait()
