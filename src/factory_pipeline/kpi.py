"""Business definitions used by tests and mirrored by Spark Gold jobs."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

IDEAL_UNITS_PER_RUNNING_MINUTE = 8.0


def calculate_machine_kpis(events: list[dict], planned_minutes: int = 60,
                           ideal_rate: float = IDEAL_UNITS_PER_RUNNING_MINUTE) -> dict[str, dict]:
    """One final sample per (machine, UTC minute); last arrival wins.

    Planned production minutes must be supplied from a real shift calendar in production.
    Missing expected samples count as non-operating time; coverage is shown separately.
    """
    if planned_minutes <= 0 or ideal_rate <= 0:
        raise ValueError("planned_minutes and ideal_rate must be positive")
    latest = {}
    for event in events:
        if event.get("status") not in {"RUNNING", "IDLE", "DOWN"}:
            continue
        t = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        minute = t.astimezone(timezone.utc).replace(second=0, microsecond=0)
        latest[(event["machine_id"], minute)] = event
    groups = defaultdict(list)
    for (machine, _), event in latest.items():
        groups[machine].append(event)
    result = {}
    for machine, rows in sorted(groups.items()):
        running = sum(e["status"] == "RUNNING" for e in rows)
        units = sum(e["units_produced"] for e in rows)
        defects = sum(e["defect_count"] for e in rows)
        availability = min(1.0, running / planned_minutes)
        performance = min(1.0, units / (ideal_rate * running)) if running else 0.0
        quality = (units - defects) / units if units else 0.0
        result[machine] = {
            "observed_minutes": len(rows),
            "planned_minutes": planned_minutes,
            "running_minutes": running,
            "units": units,
            "defects": defects,
            "throughput_units_per_planned_hour": units * 60 / planned_minutes,
            "sample_coverage": min(1.0, len(rows) / planned_minutes),
            "availability": availability,
            "performance": performance,
            "quality": quality,
            "defect_rate": defects / units if units else 0.0,
            "oee": availability * performance * quality,
        }
    return result
