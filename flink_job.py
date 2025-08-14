import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


# ---------- Sinks ----------
def create_sessions_sink_postgres(t_env):
    table_name = 'sessions_by_ip_host'
    ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end   TIMESTAMP(3),
            ip            VARCHAR,
            host          VARCHAR,
            events_in_session BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(ddl)
    return table_name


def create_avg_techcreator_sink_postgres(t_env):
    table_name = 'avg_session_events_techcreator'
    ddl = f"""
        CREATE TABLE {table_name} (
            metric VARCHAR,
            avg_events_per_session DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(ddl)
    return table_name


def create_avg_per_host_sink_postgres(t_env):
    table_name = 'avg_session_events_per_host'
    ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            avg_events_per_session DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(ddl)
    return table_name


# ---------- Source ----------
def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    ddl = f"""
        CREATE TABLE {table_name} (
            ip STRING,
            event_time STRING,
            referrer STRING,
            host STRING,
            url STRING,
            geodata STRING,
            event_ts AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' =
              'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(ddl)
    return table_name


# ---------- Job ----------
def log_sessionization():
    env = StreamExecutionEnvironment.get_execution_environment()
    # checkpointing of 10 ms is too aggressive for production; keep but feel free to raise
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_processed_events_source_kafka(t_env)

        sessions_sink = create_sessions_sink_postgres(t_env)
        tech_avg_sink = create_avg_techcreator_sink_postgres(t_env)
        host_avg_sink = create_avg_per_host_sink_postgres(t_env)

        # -------- SESSIONIZE by ip + host with a 5-minute gap --------
        sessions = (
            t_env.from_path(source_table)
                .window(Session.with_gap(lit(5).minutes).on(col("event_ts")).alias("s"))
                .group_by(col("s"), col("ip"), col("host"))
                .select(
                    col("s").start.alias("session_start"),
                    col("s").end.alias("session_end"),
                    col("ip"),
                    col("host"),
                    col("*").count.alias("events_in_session")
                )
        )

        # Persist per-session rows (optional but useful for audits/BI)
        sessions.execute_insert(sessions_sink)

        # -------- Q1: Average # events/session for “Tech Creator” --------
        (sessions
            .filter(col("host").like("%techcreator%"))
            .group_by()
            .select(
                lit("avg_session_events_techcreator").alias("metric"),
                col("events_in_session").avg.alias("avg_events_per_session")
            )
            .execute_insert(tech_avg_sink)
        )

        # -------- Q2: Compare averages for specific hosts --------
        target_hosts = [
            "zachwilson.techcreator.io",
            "zachwilson.tech",
            "lulu.techcreator.io"
        ]
        (sessions
            .filter(col("host").in_(*target_hosts))
            .group_by(col("host"))
            .select(
                col("host"),
                col("events_in_session").avg.alias("avg_events_per_session")
            )
            .execute_insert(host_avg_sink)
            .wait()
        )

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_sessionization()
