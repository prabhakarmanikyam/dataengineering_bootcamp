
from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from datetime import datetime, date
from ..jobs.device_activity_list import do_device_activity_list_transformation

DeviceEvent = namedtuple("DeviceEvent", "user_id event_time device_id")
DeviceInfo = namedtuple("DeviceInfo", "device_id browser_type")
UserDevice = namedtuple("UserDevice", "user_id browser_type device_activity_date_list date")
ExpectedResult = namedtuple("ExpectedResult", "user_id browser_type device_activity_date_list date")


def test_device_activity_list(spark):
    # Inputs
    user_devices_df = spark.createDataFrame([
        UserDevice("user1", "chrome", [date(2023, 1, 31)], date(2023, 1, 31)),
        UserDevice("user2", "firefox", [date(2023, 1, 30)], date(2023, 1, 30))
    ])

    events_df = spark.createDataFrame([
        DeviceEvent("user1", datetime.strptime("2023-01-31", "%Y-%m-%d"), "dev1"),
        DeviceEvent("user2", datetime.strptime("2023-01-30", "%Y-%m-%d"), "dev2")
    ])

    devices_df = spark.createDataFrame([
        DeviceInfo("dev1", "chrome"),
        DeviceInfo("dev2", "firefox")
    ])

    # Expected Output
    expected_df = spark.createDataFrame([
        ExpectedResult("user1", "chrome", [date(2023, 1, 31)], date(2023, 1, 31)),
        ExpectedResult("user2", "firefox", [date(2023, 1, 30)], date(2023, 1, 30))
    ])

    # Actual Output
    actual_df = do_device_activity_list_transformation(spark, user_devices_df, events_df, devices_df)

    # Assert
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_column_order=True)
