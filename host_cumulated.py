from pyspark.sql import SparkSession

query = """

With yesterday as(
	select * 
	from hosts_cumulated 
	where c_date = DATE('2023-01-02')
),

today as(
	select host,
		DATE(cast(event_time as TIMESTAMP)) as date_active
		from events
		where DATE(cast(event_time as TIMESTAMP)) = DATE('2023-01-03')
		group by host, DATE(cast(event_time as TIMESTAMP))
)
select 
		coalesce(t.host, y.host) as host,
		case when y.host_activity_datelist is null then ARRAY(t.date_active)
			when t.date_active is null then y.host_activity_datelist
			else ARRAY(t.date_active) || y.host_activity_datelist
			END as host_activity_datelist,
			coalesce(t.date_active, y.c_date ) as c_date
		from today t full outer join yesterday y on t.host = y.host

"""


def do_host_activity_list_transformation(spark, hosts_df, events_df):
    hosts_df.createOrReplaceTempView("hosts_cumulated")
    events_df.createOrReplaceTempView("events")
    return spark.sql(query)     

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("host_activity_list") \
        .getOrCreate()
    
    hosts_df = spark.table("hosts_cumulated")
    events_df = spark.table("events")
    output_df = do_host_activity_list_transformation(spark, hosts_df, events_df)
    output_df.write.mode("overwrite").saveAsTable("hosts_activity_list")