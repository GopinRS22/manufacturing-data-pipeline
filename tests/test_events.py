from datetime import datetime, timezone
from factory_pipeline.events import MACHINES, demo_events, new_event

START = datetime(2026, 10, 27, 10, 0, tzinfo=timezone.utc)


def test_ten_machines_sixty_samples():
    rows = demo_events(start=START)
    assert len(rows) == 600
    assert set(r["machine_id"] for r in rows) == set(MACHINES)
    assert all(r["units_produced"] >= r["defect_count"] >= 0 for r in rows)
    assert all(r["units_produced"] == 0 for r in rows if r["status"] != "RUNNING")


def test_failure_injection_preserves_duplicate_and_late_record():
    rows = demo_events(start=START, inject=frozenset({"duplicate", "late", "schema", "invalid"}))
    assert len(rows) == 601
    ids = [r["event_id"] for r in rows]
    assert len(set(ids)) == 600
    late = next(i for i, r in enumerate(rows) if r["machine_id"] == "M-01" and r["timestamp"].endswith("10:12:00Z"))
    assert late > 420  # held until after minute 42 is published
    assert sum("firmware_version" in e for e in rows) == 1
    assert sum("temp_c" in e and "temperature_c" not in e for e in rows) == 1
    assert sum(e["defect_count"] > e["units_produced"] for e in rows) == 1
