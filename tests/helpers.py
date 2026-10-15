"""Общие хелперы тестов: площадка на одном меридиане, чтобы расстояния были точными."""
from vqueue.codec import parse_iso
from vqueue.config import Params, Point, SiteConfig
from vqueue.engine import DecisionEvent, Engine, QueueEvent, Telemetry
from vqueue.geo import offset_north

BASE = parse_iso("2026-09-15T12:00:00Z")      # «сейчас» из примера в задании
LAT0, LON0 = 49.1288, 142.6601


def pos(north_m: float) -> Point:
    lat, lon = offset_north(LAT0, LON0, north_m)
    return Point(lat, lon)


def make_cfg(stations=None, units=None, dump_north=-3000.0, **params) -> SiteConfig:
    """stations: {id: смещение на север, м}; units: {unit: своя станция}."""
    stations = stations or {"S1": 0.0, "S2": 3300.0}
    units = units or {}
    return SiteConfig({k: pos(v) for k, v in stations.items()}, pos(dump_north), dict(units),
                      Params(**params))


def m(unit: str, ts: int, north_m: float, speed: float = 0.0) -> Telemetry:
    p = pos(north_m)
    return Telemetry(unit, ts, p.lat, p.lon, speed)


def feed(engine: Engine, *msgs: Telemetry) -> list:
    out = []
    for x in msgs:
        out.extend(engine.process(x))
    return out


def decisions(events) -> list[DecisionEvent]:
    return [e for e in events if isinstance(e, DecisionEvent)]


def queues(events, station=None) -> list[QueueEvent]:
    return [e for e in events if isinstance(e, QueueEvent) and (station is None or e.station_uuid == station)]


def rows(entries):
    """Очередь в виде кортежей (unit, eta, start, free_at, wait) для сравнения."""
    return [(e.unit_uuid, e.eta, e.service_start, e.free_at, e.wait_seconds) for e in entries]
