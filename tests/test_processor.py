import pytest
from pyspark.sql import SparkSession

from spark_streaming_processor import SCHEMA, add_alert_columns


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _row(**overrides):
    base = {name: None for name in SCHEMA.fieldNames()}
    base.update(
        temperature_celsius=50.0,
        vibration_hz=1.0,
        courant_ampere=20.0,
        battery_percent=80.0,
        rssi_dbm=-60,
    )
    base.update(overrides)
    return tuple(base[name] for name in SCHEMA.fieldNames())


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({}, "OK"),
        ({"temperature_celsius": 70.0}, "WARNING"),
        ({"vibration_hz": 3.5}, "WARNING"),
        ({"battery_percent": 10.0}, "WARNING"),
        ({"temperature_celsius": 85.0}, "CRITICAL"),
        ({"vibration_hz": 5.0, "battery_percent": 10.0}, "CRITICAL"),
        ({"courant_ampere": 36.0}, "CRITICAL"),
    ],
)
def test_status(spark, overrides, expected):
    df = spark.createDataFrame([_row(**overrides)], SCHEMA)
    assert add_alert_columns(df).first()["status"] == expected


def test_flags(spark):
    df = spark.createDataFrame([_row(rssi_dbm=-85, battery_percent=5.0)], SCHEMA)
    row = add_alert_columns(df).first()
    assert row["is_signal_weak"] is True
    assert row["is_battery_low"] is True
    assert row["is_temp_critical"] is False
