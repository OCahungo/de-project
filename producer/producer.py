"""Simulated IoT sensors: sends JSON temperature/humidity readings to Kafka."""

from __future__ import annotations

import json
import math
import os
import random
import time
from datetime import UTC, datetime

LOCATIONS = ["warehouse-a", "warehouse-b", "office", "server-room", "cold-storage"]
BASE_TEMP_C = {
    "warehouse-a": 18.0,
    "warehouse-b": 17.0,
    "office": 22.0,
    "server-room": 26.0,
    "cold-storage": 4.0,
}


def make_sensors(n: int) -> list[dict]:
    return [
        {"sensor_id": f"sensor-{i:03d}", "location": LOCATIONS[i % len(LOCATIONS)]}
        for i in range(1, n + 1)
    ]


def generate_reading(
    sensor: dict, now: datetime, rng: random.Random, anomaly_rate: float = 0.02
) -> dict:
    """Build one reading. Occasionally injects bad data so dbt tests have work to do."""
    hour = now.hour + now.minute / 60
    daily = math.sin((hour - 6) / 24 * 2 * math.pi)
    temperature = BASE_TEMP_C[sensor["location"]] + 2.0 * daily + rng.gauss(0, 0.4)
    humidity = 55 - 6 * daily + rng.gauss(0, 1.5)
    reading = {
        "sensor_id": sensor["sensor_id"],
        "location": sensor["location"],
        "event_ts": now.isoformat(),
        "temperature_c": round(temperature, 2),
        "humidity_pct": round(min(max(humidity, 5.0), 95.0), 2),
        "battery_pct": round(rng.uniform(20, 100), 1),
    }
    if rng.random() < anomaly_rate:
        kind = rng.choice(["temp_spike", "bad_humidity", "missing_temp"])
        if kind == "temp_spike":
            reading["temperature_c"] = round(rng.uniform(80, 120), 2)
        elif kind == "bad_humidity":
            reading["humidity_pct"] = round(rng.uniform(101, 150), 2)
        else:
            reading["temperature_c"] = None
    return reading


def main() -> None:
    from confluent_kafka import Producer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("TOPIC", "iot.readings")
    n_sensors = int(os.getenv("NUM_SENSORS", "20"))
    interval = float(os.getenv("INTERVAL_SECONDS", "2"))
    dup_rate = float(os.getenv("DUPLICATE_RATE", "0.01"))

    def on_delivery(err, _msg):
        if err is not None:
            print(f"delivery failed: {err}", flush=True)

    producer = Producer({"bootstrap.servers": bootstrap})
    sensors = make_sensors(n_sensors)
    rng = random.Random()
    print(f"producing {n_sensors} sensors every {interval}s to {topic} @ {bootstrap}", flush=True)

    sent = 0
    while True:
        now = datetime.now(UTC)
        for sensor in sensors:
            payload = json.dumps(generate_reading(sensor, now, rng)).encode()
            producer.produce(topic, key=sensor["sensor_id"], value=payload, on_delivery=on_delivery)
            sent += 1
            if rng.random() < dup_rate:  # duplicate delivery, to test deduplication
                producer.produce(topic, key=sensor["sensor_id"], value=payload)
        producer.poll(0)
        producer.flush(5)
        if sent % (n_sensors * 15) == 0:
            print(f"sent {sent} messages", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
