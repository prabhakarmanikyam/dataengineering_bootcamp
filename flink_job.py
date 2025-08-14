import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session

SESSION_GAP_MIN = int(os.environ.get("SESSION_GAP_MIN", "5"))
TARGET_HOSTS = [h.strip() for h in os.environ.get(
    "TARGET_HOSTS",
    "zachwilson.techcreator.io,zachwilson.tech,lulu.techcreator.io"
).split(",")]

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
            'driver' = 'org.postgresql.Driver',
            'sink.max-retries' = '3',
            'sink.flush.max-rows' = '500',
            'sink.flush.interval' = '2s'
        )
    """
    t_env.execute_sql(ddl); return table_name

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
            'driver' = 'org.postgresql.Driver',
            'sink.max-retries' = '3'
        )
    """
    t_env.execute_sql(ddl); return table_name

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
            'driver' = 'org.postgresql.Driver',
            'sink.max-retries' = '3'
        )
    """
    t_env.execute_sql(ddl); return table_name

def log_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(int(os.environ.get("CHECKPOINT_MS", "60000")))
    env.set_parallelism(int(os.environ.get("PARALLELISM", "3")))
    env.set_restart_strategy("fixed_delay_restart", 3, 10_000)  # 3 retries, 10s delay

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # ... keep your Kafka source DDL exactly as-is ...

    source_table = create_processed_events_source_kafka(t_env)
    sessions_sink  = create_sessions_sink_postgres(t_env)
    tech_avg_sink  = create_avg_techcreator_sink_postgres(t_env)
    host_avg_sink  = create_avg_per_host_sink_postgres(t_env)

    # --- sessionize by (ip,host) with a SESSION_GAP_MIN gap ---
    sessions = (
        t_env.from_path(source_table)
            .window(Session.with_gap(lit(SESSION_GAP_MIN).minutes)
            .on(col("window_timestamp")).alias("s"))
            .group_by(col("s"), col("ip"), col("host"))
            .select(
                col("s").start.alias("session_start"),
                col("s").end.alias("session_end"),
                col("ip"),
                col("host"),
                col("*").count.alias("events_in_session")
            )
    )

    # Persist one row per session (audit-friendly)
    sessions.execute_insert(sessions_sink)

    # Q1: Average events/session for Tech Creator (any host containing 'techcreator')
    (sessions
        .filter(col("host").like("%techcreator%"))
        .group_by()
        .select(
            lit("avg_session_events_techcreator").alias("metric"),
            col("events_in_session").avg.alias("avg_events_per_session")
        )
        .execute_insert(tech_avg_sink)
    )

    # Q2: Average events/session per selected hosts
    (sessions
        .filter(col("host").in_(*TARGET_HOSTS))
        .group_by(col("host"))
        .select(
            col("host"),
            col("events_in_session").avg.alias("avg_events_per_session")
        )
        .execute_insert(host_avg_sink)
        .wait()
    )
