"""Симулятор площадки: машины ездят по прямой между точкой разгрузки и своей станцией
с постоянной скоростью, стоят на станции 200 с и соблюдают очередь.

Время — целые секунды, шаг 1 с, без часов компьютера: симуляция детерминирована при
заданном seed. Отдельно — `Noise`, который добавляет дубли и опоздания к потоку.
"""
from __future__ import annotations

import heapq
import random
from dataclasses import dataclass
from typing import Optional

from .config import Point, SiteConfig
from .models import Telemetry
from .geo import haversine_m


@dataclass
class Vehicle:
    unit: str
    home: str
    station: str                    # куда едет/где стоит сейчас
    src: Point
    dst: Point
    seg_len: float
    s: float = 0.0                  # пройдено по текущему отрезку, м
    phase: str = "to_station"       # to_station | waiting | service | to_dump
    service_end: int = 0
    stopped: bool = False

    def position(self) -> tuple[float, float]:
        f = 1.0 if self.seg_len <= 0 else min(1.0, self.s / self.seg_len)
        return (self.src.lat + (self.dst.lat - self.src.lat) * f,
                self.src.lon + (self.dst.lon - self.src.lon) * f)


class Simulator:
    def __init__(self, cfg: SiteConfig, t0: int, seed: int = 1):
        self.cfg = cfg
        self.p = cfg.params
        self.t = t0 - 1
        rng = random.Random(seed)
        self.vehicles: dict[str, Vehicle] = {}
        for unit in sorted(cfg.assignments):
            home = cfg.assignments[unit]
            v = self._segment(cfg.dump, cfg.stations[home])
            veh = Vehicle(unit, home, home, cfg.dump, cfg.stations[home], v)
            veh.s = rng.random() * v         # случайная точка на пути к станции
            self.vehicles[unit] = veh
        self.waiting: dict[str, list[str]] = {s: [] for s in cfg.stations}
        self.busy_until: dict[str, int] = {s: t0 - 1 for s in cfg.stations}

    @staticmethod
    def _segment(a: Point, b: Point) -> float:
        return haversine_m(a.lat, a.lon, b.lat, b.lon)

    def _go(self, v: Vehicle, src: Point, dst: Point, phase: str) -> None:
        v.src, v.dst, v.seg_len, v.s, v.phase = src, dst, self._segment(src, dst), 0.0, phase

    def redirect(self, unit: str, station: str) -> bool:
        """Водитель принял рекомендацию: едет к другой станции прямо из текущей точки."""
        v = self.vehicles.get(unit)
        if v is None or v.phase != "to_station" or station == v.station:
            return False
        lat, lon = v.position()
        self._go(v, Point(lat, lon), self.cfg.stations[station], "to_station")
        v.station = station
        return True

    def _move(self, v: Vehicle) -> bool:
        """Сдвигает машину на шаг; True, если дошла до конца отрезка."""
        step = min(self.p.speed_ms, v.seg_len - v.s)
        v.s += step
        v.stopped = False
        return v.s >= v.seg_len - 1e-9

    def tick(self) -> list[Telemetry]:
        """Продвигает время на секунду и возвращает телеметрию всех машин с ts = новое время."""
        self.t += 1
        t, p, cfg = self.t, self.p, self.cfg
        for unit in sorted(self.vehicles):
            v = self.vehicles[unit]
            if v.phase == "service" and t >= v.service_end:
                self._go(v, cfg.stations[v.station], cfg.dump, "to_dump")
            if v.phase == "to_station":
                if self._move(v):
                    v.phase, v.stopped = "waiting", True
                    self.waiting[v.station].append(unit)
            elif v.phase == "to_dump":
                if self._move(v):                       # разгрузка мгновенная
                    self._go(v, cfg.dump, cfg.stations[v.home], "to_station")
                    v.station = v.home
        for sid in cfg.stations:                        # станция принимает следующего
            q = self.waiting[sid]
            if q and t >= self.busy_until[sid]:
                v = self.vehicles[q.pop(0)]
                v.phase, v.service_end, v.stopped = "service", t + p.service_s, True
                self.busy_until[sid] = t + p.slot_s
        out = []
        for unit in sorted(self.vehicles):
            v = self.vehicles[unit]
            lat, lon = v.position()
            moving = v.phase in ("to_station", "to_dump") and not v.stopped
            out.append(Telemetry(unit, t, lat, lon, p.speed_kmh if moving else 0.0))
        return out


class Noise:
    """Дубли и опоздания (до `max_late` секунд) на пути от машины к брокеру."""

    def __init__(self, dup_rate: float = 0.0, late_rate: float = 0.0, max_late: int = 60, seed: int = 7):
        self.dup_rate, self.late_rate, self.max_late = dup_rate, late_rate, max_late
        self._rng = random.Random(seed)
        self._heap: list[tuple[int, int, Telemetry]] = []
        self._seq = 0

    def _push(self, at: int, m: Telemetry) -> None:
        self._seq += 1
        heapq.heappush(self._heap, (at, self._seq, m))

    def push(self, t: int, msgs: list[Telemetry]) -> list[Telemetry]:
        for m in msgs:
            delay = self._rng.randint(1, self.max_late) if self._rng.random() < self.late_rate else 0
            self._push(t + delay, m)
            if self._rng.random() < self.dup_rate:
                self._push(t + delay + self._rng.randint(0, 5), m)
        return self.pop_due(t)

    def pop_due(self, t: Optional[int]) -> list[Telemetry]:
        out = []
        while self._heap and (t is None or self._heap[0][0] <= t):
            out.append(heapq.heappop(self._heap)[2])
        return out

    def flush(self) -> list[Telemetry]:
        return self.pop_due(None)
