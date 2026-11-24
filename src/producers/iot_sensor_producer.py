#!/usr/bin/env python3
"""Producer Kafka : simule des capteurs industriels et publie les mesures brutes.

Le producer n'envoie que ce que ferait un vrai capteur (mesures + état du
boîtier). Les alertes et le statut sont calculés plus loin, dans Spark.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "iot-sensor-data")
SEND_INTERVAL_SEC = float(os.getenv("SEND_INTERVAL_SEC", "2"))

# Probabilité qu'une mesure parte en anomalie (pic) à un envoi donné
ANOMALY_RATE = 0.01

# base = valeur nominale de la machine, noise = écart-type du bruit de mesure
MACHINES = [
    {"id": "MCH-101", "location": "Plant_Dakar_1", "firmware": "1.2.0",
     "temp": 55.0, "vib": 1.8, "amp": 22.0},
    {"id": "MCH-102", "location": "Plant_Dakar_1", "firmware": "1.2.1",
     "temp": 58.0, "vib": 2.1, "amp": 24.0},
    {"id": "MCH-105", "location": "Plant_Thies_2", "firmware": "1.1.9",
     "temp": 52.0, "vib": 1.5, "amp": 20.0},
    {"id": "MCH-110", "location": "Plant_SaintLouis_1", "firmware": "1.2.0",
     "temp": 59.0, "vib": 2.2, "amp": 23.0},
]

# État propre à chaque machine : numéro de séquence, uptime, batterie
_state = {
    m["id"]: {
        "seq": 0,
        "uptime": random.randint(3600, 86400 * 7),
        "battery": random.uniform(60.0, 100.0),
    }
    for m in MACHINES
}


def create_producer(max_retries: int = 15, retry_delay: int = 5) -> KafkaProducer:
    """Se connecte à Kafka, en réessayant tant que le broker démarre."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[+] Connecté à Kafka → {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"[-] Kafka non disponible (tentative {attempt}/{max_retries})…")
            time.sleep(retry_delay)
    raise RuntimeError(f"Impossible de se connecter à Kafka après {max_retries} tentatives")


def _measure(base: float, noise: float, spike: float) -> float:
    """Valeur nominale + bruit gaussien, avec un pic occasionnel."""
    value = random.gauss(base, noise)
    if random.random() < ANOMALY_RATE:
        value += spike
    return value


def generate_sensor_data(machine: dict) -> dict:
    """Construit un message de mesures brutes pour une machine."""
    st = _state[machine["id"]]
    st["seq"] += 1
    st["uptime"] += int(SEND_INTERVAL_SEC)
    # La batterie se vide lentement ; on la remplace quand elle est presque à plat
    st["battery"] -= random.uniform(0.0, 0.05)
    if st["battery"] < 12.0:
        st["battery"] = 100.0

    current = max(0.0, _measure(machine["amp"], 2.5, 15.0))
    voltage = random.gauss(225.0, 4.0)
    power_kw = current * voltage / 1000
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "timestamp": ts,
        "event_time": int(now.timestamp() * 1000),
        "machine_id": machine["id"],
        "plant_location": machine["location"],
        "temperature_celsius": round(_measure(machine["temp"], 3.0, 30.0), 2),
        "humidity_percent": round(min(100.0, max(0.0, random.gauss(55.0, 8.0))), 1),
        "vibration_hz": round(max(0.0, _measure(machine["vib"], 0.4, 3.0)), 2),
        "pressure_bar": round(max(0.0, random.gauss(3.0, 0.4)), 2),
        "courant_ampere": round(current, 2),
        "tension_volt": round(voltage, 1),
        "puissance_kw": round(power_kw, 2),
        "consommation_kwh": round(power_kw * SEND_INTERVAL_SEC / 3600, 4),
        "battery_percent": round(st["battery"], 1),
        "rssi_dbm": random.randint(-90, -40),
        "uptime_seconds": st["uptime"],
        "firmware_version": machine["firmware"],
        "message_seq": st["seq"],
        "last_seen": ts,
    }


def main():
    print(f"Producer IoT | broker={KAFKA_BROKER} topic={TOPIC_NAME} "
          f"intervalle={SEND_INTERVAL_SEC}s")
    producer = create_producer()

    try:
        while True:
            for machine in MACHINES:
                data = generate_sensor_data(machine)
                producer.send(TOPIC_NAME, value=data)
                print(
                    f"[{data['timestamp']}] {data['machine_id']} | "
                    f"{data['temperature_celsius']:5.1f}°C | "
                    f"{data['vibration_hz']:4.2f} Hz | "
                    f"{data['courant_ampere']:5.1f} A | "
                    f"bat {data['battery_percent']:5.1f}%"
                )
            producer.flush()
            time.sleep(SEND_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
