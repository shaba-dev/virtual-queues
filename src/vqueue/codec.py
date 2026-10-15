"""Сериализация: разбор telemetry.v1 и формирование queue.v1 / decision.v1."""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any, Optional

from .models import DecisionEvent, QueueEvent, Telemetry


def iso(ts: Optional[int]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: str) -> int:
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())


def _num(v: Any) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ValueError("не число")
    if not math.isfinite(v):
        raise ValueError("не конечное число")
    return float(v)


def parse_telemetry(raw: bytes | str | dict) -> Optional[Telemetry]:
    """Разбор сообщения telemetry.v1. Некорректное сообщение -> None (его пропускают и считают)."""
    try:
        d = json.loads(raw) if isinstance(raw, (bytes, str)) else raw
        unit = d["unit_uuid"]
        if not isinstance(unit, str) or not unit:
            raise ValueError("unit_uuid")
        ts = _num(d["ts"])
        lat, lon, speed = _num(d["lat"]), _num(d["lon"]), _num(d["speed_kmh"])
        if not (-90 <= lat <= 90 and -180 <= lon <= 180 and speed >= 0):
            raise ValueError("диапазон")
        return Telemetry(unit, int(ts), lat, lon, speed)
    except (ValueError, KeyError, TypeError):
        return None


def telemetry_to_dict(m: Telemetry) -> dict:
    return {"unit_uuid": m.unit_uuid, "ts": m.ts, "lat": round(m.lat, 7),
            "lon": round(m.lon, 7), "speed_kmh": round(m.speed_kmh, 1)}


def queue_to_dict(e: QueueEvent) -> dict:
    return {
        "station_uuid": e.station_uuid,
        "at": iso(e.at),
        "queue": [
            {"unit_uuid": q.unit_uuid, "eta": iso(q.eta), "service_start": iso(q.service_start),
             "free_at": iso(q.free_at), "wait_seconds": q.wait_seconds}
            for q in e.queue
        ],
    }


def decision_to_dict(e: DecisionEvent) -> dict:
    d: dict = {"unit_uuid": e.unit_uuid, "at": iso(e.at), "result": e.result}
    if e.result == "recommended":
        d.update(from_station=e.from_station, to_station=e.to_station, gain_seconds=e.gain_seconds)
    else:
        d["reason"] = e.reason
    return d


def dumps(d: dict) -> bytes:
    return json.dumps(d, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
