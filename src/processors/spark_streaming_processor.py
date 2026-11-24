#!/usr/bin/env python3
"""Job Spark Structured Streaming : Kafka → calcul des alertes → Elasticsearch.

Les packages Kafka et Elasticsearch ne sont pas déclarés ici : ils sont passés
à spark-submit (voir docker/spark/Dockerfile et le README).
"""

import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-sensor-data")
ES_NODES = os.getenv("ES_NODES", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_INDEX = os.getenv("ES_INDEX", "iot-telemetry-metrics")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark_es_checkpoint")
# Laisse au producer le temps de créer le topic avant la première lecture
STARTUP_DELAY_SEC = int(os.getenv("STARTUP_DELAY_SEC", "20"))

# Seuils d'alerte : au-dessus -> CRITICAL, au-dessus du seuil "warn" -> WARNING
TEMP_CRIT, TEMP_WARN = 80.0, 65.0
VIB_CRIT, VIB_WARN = 4.5, 3.0
CURRENT_CRIT, CURRENT_WARN = 35.0, 28.0
BATTERY_LOW = 20.0
RSSI_WEAK = -80

SCHEMA = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("event_time", LongType()),
        StructField("machine_id", StringType()),
        StructField("plant_location", StringType()),
        StructField("temperature_celsius", DoubleType()),
        StructField("humidity_percent", DoubleType()),
        StructField("vibration_hz", DoubleType()),
        StructField("pressure_bar", DoubleType()),
        StructField("courant_ampere", DoubleType()),
        StructField("tension_volt", DoubleType()),
        StructField("puissance_kw", DoubleType()),
        StructField("consommation_kwh", DoubleType()),
        StructField("battery_percent", DoubleType()),
        StructField("rssi_dbm", IntegerType()),
        StructField("uptime_seconds", IntegerType()),
        StructField("firmware_version", StringType()),
        StructField("message_seq", LongType()),
        StructField("last_seen", StringType()),
    ]
)


def add_alert_columns(df: DataFrame) -> DataFrame:
    """Ajoute les drapeaux d'alerte et le statut global de chaque mesure."""
    df = (
        df.withColumn("is_temp_critical", F.col("temperature_celsius") > TEMP_CRIT)
        .withColumn("is_vibration_critical", F.col("vibration_hz") > VIB_CRIT)
        .withColumn("is_courant_critical", F.col("courant_ampere") > CURRENT_CRIT)
        .withColumn("is_battery_low", F.col("battery_percent") < BATTERY_LOW)
        .withColumn("is_signal_weak", F.col("rssi_dbm") < RSSI_WEAK)
    )
    is_critical = (
        F.col("is_temp_critical")
        | F.col("is_vibration_critical")
        | F.col("is_courant_critical")
    )
    is_warning = (
        (F.col("temperature_celsius") > TEMP_WARN)
        | (F.col("vibration_hz") > VIB_WARN)
        | (F.col("courant_ampere") > CURRENT_WARN)
        | F.col("is_battery_low")
    )
    return df.withColumn(
        "status",
        F.when(is_critical, "CRITICAL").when(is_warning, "WARNING").otherwise("OK"),
    )


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.appName("IoTMaintenancePipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_to_elasticsearch(batch_df: DataFrame, epoch_id: int):
    """Écrit un micro-batch dans Elasticsearch."""
    if batch_df.isEmpty():
        return
    (
        batch_df.write.format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_NODES)
        .option("es.port", ES_PORT)
        .option("es.resource", ES_INDEX)
        .option("es.nodes.wan.only", "true")
        .mode("append")
        .save()
    )
    print(f"[+] Batch {epoch_id} écrit dans '{ES_INDEX}'")


def main():
    print(f"Spark processor | kafka={KAFKA_BROKER}/{KAFKA_TOPIC} "
          f"-> es={ES_NODES}:{ES_PORT}/{ES_INDEX}")
    time.sleep(STARTUP_DELAY_SEC)
    spark = create_spark_session()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    measures = (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("m"))
        .select("m.*")
    )

    query = (
        add_alert_columns(measures)
        .writeStream.foreachBatch(write_to_elasticsearch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
