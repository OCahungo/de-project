import random
from datetime import UTC, datetime

from producer.producer import generate_reading, make_sensors

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def test_make_sensors_unique_ids():
    sensors = make_sensors(20)
    assert len({s["sensor_id"] for s in sensors}) == 20


def test_normal_reading_in_range():
    sensor = make_sensors(1)[0]
    r = generate_reading(sensor, NOW, random.Random(1), anomaly_rate=0.0)
    assert r["sensor_id"] == sensor["sensor_id"]
    assert -40 < r["temperature_c"] < 60
    assert 0 <= r["humidity_pct"] <= 100


def test_anomalies_are_injected():
    sensor = make_sensors(1)[0]
    rng = random.Random(7)
    readings = [generate_reading(sensor, NOW, rng, anomaly_rate=1.0) for _ in range(50)]
    bad = [
        r
        for r in readings
        if r["temperature_c"] is None or r["temperature_c"] > 60 or r["humidity_pct"] > 100
    ]
    assert len(bad) == 50
