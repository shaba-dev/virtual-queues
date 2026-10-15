"""Конфигурация площадки: станции, точка разгрузки, закрепление машин, константы."""
from __future__ import annotations

import json
from dataclasses import dataclass, field, fields, replace
from pathlib import Path


DEFAULT_BOOTSTRAP = "127.0.0.1:9092"   # 127.0.0.1, а не localhost: на Windows localhost сначала пробуется как IPv6


@dataclass(frozen=True)
class Point:
    lat: float
    lon: float


@dataclass(frozen=True)
class Params:
    service_s: int = 200            # время обслуживания одной машины
    maneuver_s: int = 30            # манёвр между машинами
    speed_kmh: float = 36.0         # расчётная скорость движения
    radius_m: float = 50.0          # радиус станции и точки разгрузки
    stand_speed_kmh: float = 1.0    # порог «машина стоит»
    decision_radius_m: float = 1500.0
    horizon_s: int = 1800           # горизонт очереди
    fresh_s: int = 30               # позиция свежая, если не старше
    min_gain_s: int = 60            # минимальный выигрыш для рекомендации

    @property
    def slot_s(self) -> int:
        """Сколько машина занимает станцию: обслуживание + манёвр (230 с)."""
        return self.service_s + self.maneuver_s

    @property
    def speed_ms(self) -> float:
        return self.speed_kmh / 3.6


@dataclass(frozen=True)
class SiteConfig:
    stations: dict[str, Point]
    dump: Point
    assignments: dict[str, str]     # unit_uuid -> station_uuid (своя станция)
    params: Params = field(default_factory=Params)

    def __post_init__(self) -> None:
        if not self.stations:
            raise ValueError("нужна хотя бы одна станция")
        for unit, st in self.assignments.items():
            if st not in self.stations:
                raise ValueError(f"машина {unit} закреплена за неизвестной станцией {st}")

    @classmethod
    def from_dict(cls, d: dict) -> "SiteConfig":
        stations = {s["id"]: Point(float(s["lat"]), float(s["lon"])) for s in d["stations"]}
        dump = Point(float(d["dump"]["lat"]), float(d["dump"]["lon"]))
        if "assignments" in d:
            assignments = dict(d["assignments"])
        else:  # компактная форма: {"S1": ["T-01", ...]}
            assignments = {u: st for st, units in d["units_by_station"].items() for u in units}
        known = {f.name for f in fields(Params)}
        extra = d.get("params", {})
        bad = set(extra) - known
        if bad:
            raise ValueError(f"неизвестные параметры: {sorted(bad)}")
        return cls(stations, dump, assignments, replace(Params(), **extra))

    @classmethod
    def load(cls, path: str | Path) -> "SiteConfig":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
