from pyspark.sql import SparkSession

query = """
with yesterday as(
		select * from user_devices_cumulated
		where date = DATE('2023-01-30')
),
today as(
		SELECT cast(e.user_id as string) as user_id,
		DATE(CAST(e.event_time as TIMESTAMP)) as date_active,
		d.browser_type as browser_type
		from
		events e JOIN devices d ON d.device_id = e.device_id where DATE(CAST(e.event_time as TIMESTAMP)) = DATE('2023-01-31') and e.user_id is not null
		group by e.user_id, DATE(CAST(e.event_time as TIMESTAMP)), d.browser_type
)
select 
		coalesce(t.user_id,y.user_id) as user_id,
		coalesce(t.browser_type, y.browser_type) as browser_type,
		case when y.device_activity_date_list is null 
		then array(t.date_active) 
		when t.date_active is null then y.device_activity_date_list
		else array(t.date_active) || y.device_activity_date_list
		end as device_activity_date_list,
		coalesce(t.date_active,y.date) as date
from today t full outer join yesterday y on t.user_id = y.user_id and t.browser_type = y.browser_type

"""


def do_device_activity_list_transformation(spark, user_devices_df,event_df,devices_df):
    user_devices_df.createOrReplaceTempView("user_devices_cumulated")
    event_df.createOrReplaceTempView("events")
    devices_df.createOrReplaceTempView("devices")
    return spark.sql(query) 


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("device_activity_list") \
        .getOrCreate()
    
    user_devices_df = spark.table("user_devices_cumulated")
    event_df = spark.table("events")
    devices_df = spark.table("devices")
    output_df = do_device_activity_list_transformation(spark, user_devices_df, event_df, devices_df)
    output_df.write.mode("overwrite").saveAsTable("user_devices_activity_list")