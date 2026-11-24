import iot_sensor_producer as prod


def test_message_contains_raw_measures_only():
    msg = prod.generate_sensor_data(prod.MACHINES[0])
    assert msg["machine_id"] == "MCH-101"
    assert "status" not in msg
    assert not any(key.startswith("is_") for key in msg)


def test_sequence_increments_per_machine():
    machine = prod.MACHINES[1]
    first = prod.generate_sensor_data(machine)["message_seq"]
    second = prod.generate_sensor_data(machine)["message_seq"]
    assert second == first + 1


def test_values_stay_in_physical_range():
    for _ in range(500):
        for machine in prod.MACHINES:
            msg = prod.generate_sensor_data(machine)
            assert 0 <= msg["humidity_percent"] <= 100
            assert msg["courant_ampere"] >= 0
            assert msg["vibration_hz"] >= 0
            assert 0 < msg["battery_percent"] <= 100
