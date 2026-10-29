"""Pure-Python synthetic one-minute interval factory telemetry (no customer data)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import random
import uuid

MACHINES = tuple(f"M-{i:02d}" for i in range(1, 11))
STATUSES = ("RUNNING", "IDLE", "DOWN")


def previous_complete_hour(now: datetime | None = None) -> datetime:
    now = now or datetime.now(timezone.utc)
    return now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)


def new_event(machine_id: str, ts: datetime, rng: random.Random) -> dict:
    if machine_id not in MACHINES:
        raise ValueError(f"Unknown machine {machine_id}")
    status = rng.choices(STATUSES, weights=[82, 10, 8])[0]
    units = rng.randint(5, 8) if status == "RUNNING" else 0
    defects = sum(rng.random() < 0.03 for _ in range(units))
    base_temp = 70 if status == "RUNNING" else (45 if status == "IDLE" else 37)
    return {
        "event_id": str(uuid.uuid4()),
        "machine_id": machine_id,
        "timestamp": ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "temperature_c": round(base_temp + rng.uniform(-8, 8), 1),
        "status": status,
        # Both counts are per sample interval, NOT cumulative machine counters.
        "units_produced": units,
        "defect_count": defects,
    }


def demo_events(*, ticks: int = 60, seed: int = 42, start: datetime | None = None,
                inject: frozenset[str] = frozenset()) -> list[dict]:
    """Emission-order list. 'late' holds one record 30 simulated minutes.

    Each tick represents one simulated minute (replay can be accelerated).
    """
    if ticks < 1:
        raise ValueError("ticks must be >= 1")
    if not inject <= {"duplicate", "late", "schema", "invalid"}:
        raise ValueError(f"Unknown injection(s): {inject}")
    rng = random.Random(seed)
    start = start or previous_complete_hour()
    rows: list[dict] = []
    held_late = None
    duplicate = None
    for tick in range(ticks):
        ts = start + timedelta(minutes=tick)
        for machine in MACHINES:
            event = new_event(machine, ts, rng)
            if "duplicate" in inject and tick == 8 and machine == "M-02":
                duplicate = event.copy()
            if "late" in inject and tick == 12 and machine == "M-01":
                held_late = event
                continue
            if "schema" in inject and tick == 20 and machine == "M-03":
                event["firmware_version"] = "v2.1"  # additive -> accepted
            if "schema" in inject and tick == 21 and machine == "M-03":
                event["temp_c"] = event.pop("temperature_c")  # breaking -> invalid
            if "invalid" in inject and tick == 22 and machine == "M-04":
                event["defect_count"] = event["units_produced"] + 1
            rows.append(event)
        if tick == 40 and duplicate is not None:
            rows.append(duplicate.copy())
        if tick == 42 and held_late is not None:
            rows.append(held_late)
            held_late = None
    if held_late is not None:
        rows.append(held_late)  # short demos still keep the held sample
    if duplicate is not None and ticks <= 40:
        rows.append(duplicate.copy())
    return rows
