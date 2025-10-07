from pyflink.table import EnvironmentSettings, TableEnvironment

# Criar Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Criar source
t_env.execute_sql("""
CREATE TABLE kafka_source (
    msg STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'test_topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'raw',
    'scan.startup.mode' = 'earliest-offset'
)
""")

# Criar sink
t_env.execute_sql("""
CREATE TABLE kafka_sink (
    msg STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'output_topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'raw'
)
""")

# Inserir do source para o sink
t_env.execute_sql("INSERT INTO kafka_sink SELECT msg FROM kafka_source")
