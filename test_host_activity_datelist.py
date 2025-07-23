from ..jobs.host_cumulated import do_host_activity_list_transformation
from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from datetime import datetime, date 

HostEvent = namedtuple("HostEvent", "host event_time")
HostActivity = namedtuple("HostActivity", "host host_activity_datelist c_date")
ExpectedResult = namedtuple("ExpectedResult", "host host_activity_datelist c_date") 

def test_host_activity_list(spark): 
    # Inputs
    # Input: Past host activity
    hosts_df = spark.createDataFrame([
        HostActivity("host1", [date(2023, 1, 2)], datetime.strptime("2023-01-02", "%Y-%m-%d")),
        HostActivity("host2", [date(2023, 1, 2)], datetime.strptime("2023-01-02", "%Y-%m-%d")),
    ])

    # Input: New events on next day
    events_df = spark.createDataFrame([
        HostEvent("host1", "2023-01-03T09:30:00"),  # New day, same host
        HostEvent("host3", "2023-01-03T10:15:00"),  # New host not in yesterday's data
    ])

    # Expected Output: Updated activity list per host
    expected_df = spark.createDataFrame([
        ExpectedResult("host1", [date(2023, 1, 3), date(2023, 1, 2)], datetime.strptime("2023-01-03", "%Y-%m-%d")),
        ExpectedResult("host2", [date(2023, 1, 2)], datetime.strptime("2023-01-02", "%Y-%m-%d")),
        ExpectedResult("host3", [date(2023, 1, 3)], datetime.strptime("2023-01-03", "%Y-%m-%d")),
    ])

    # Actual Output
    actual_df = do_host_activity_list_transformation(spark, hosts_df, events_df)
    # Assert   
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_column_order=True)