from factory_pipeline.kpi import calculate_machine_kpis


def test_oee_formula_is_availability_times_performance_times_quality():
    events = [
        {"event_id": "one", "machine_id": "M-01", "timestamp": "2026-10-27T10:00:00Z", "status": "RUNNING", "units_produced": 8, "defect_count": 1},
        {"event_id": "two", "machine_id": "M-01", "timestamp": "2026-10-27T10:01:00Z", "status": "IDLE", "units_produced": 0, "defect_count": 0},
    ]
    kpi = calculate_machine_kpis(events, planned_minutes=2)["M-01"]
    assert kpi["availability"] == 0.5
    assert kpi["performance"] == 1.0
    assert kpi["quality"] == 0.875
    assert kpi["defect_rate"] == 0.125
    assert kpi["oee"] == 0.4375
    assert kpi["throughput_units_per_planned_hour"] == 240.0


def test_last_sample_wins_without_double_counting():
    event = {"event_id": "one", "machine_id": "M-01", "timestamp": "2026-10-27T10:00:00Z", "status": "RUNNING", "units_produced": 7, "defect_count": 0}
    assert calculate_machine_kpis([event, event], planned_minutes=60)["M-01"]["units"] == 7


def test_missing_samples_reduce_availability_and_coverage():
    event = {"event_id": "one", "machine_id": "M-01", "timestamp": "2026-10-27T10:00:00Z", "status": "RUNNING", "units_produced": 8, "defect_count": 0}
    kpi = calculate_machine_kpis([event], planned_minutes=60)["M-01"]
    assert round(kpi["availability"], 5) == round(1 / 60, 5)
    assert round(kpi["sample_coverage"], 5) == round(1 / 60, 5)
