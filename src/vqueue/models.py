"""Доменные модели: входное сообщение, выходные события, состояние машин и станций.

Только данные, никакой логики расчёта (она в `engine.py`). Формат на проводе (JSON) — в `codec.py`.

Состояния машины (`UnitState.phase`) в терминах задания:
    TO_STATION  — «к станции»
    AT_STATION  — «на станции» (обслуживается или ждёт, пока станция занята другой)
    TO_DUMP     — «к разгрузке»
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

TO_STATION = "to_station"
TO_DUMP = "to_dump"
AT_STATION = "at_station"

NEG_INF = -(10 ** 18)
SNAPSHOT_VERSION = 1


@dataclass(frozen=True)
class Telemetry:
    unit_uuid: str
    ts: int
    lat: float
    lon: float
    speed_kmh: float


@dataclass(frozen=True)
class QueueEntry:
    unit_uuid: str
    eta: Optional[int]          # None — машина уже на станции
    service_start: int
    free_at: int
    wait_seconds: int


@dataclass(frozen=True)
class QueueEvent:
    station_uuid: str
    at: int
    queue: tuple[QueueEntry, ...]


@dataclass(frozen=True)
class DecisionEvent:
    unit_uuid: str
    at: int
    result: str                 # "recommended" | "rejected"
    from_station: Optional[str] = None
    to_station: Optional[str] = None
    gain_seconds: Optional[int] = None
    reason: Optional[str] = None


@dataclass
class UnitState:
    unit: str
    home: str
    target: str                         # куда машина едет сейчас (своя или рекомендованная)
    last_ts: Optional[int] = None
    lat: float = 0.0
    lon: float = 0.0
    phase: str = TO_STATION
    near: Optional[str] = None          # станция, в радиусе которой машина сейчас
    near_since: Optional[int] = None    # ts первого сообщения внутри этого радиуса
    station: Optional[str] = None       # станция, на которой машина (phase == AT_STATION)
    arrival_ts: Optional[int] = None    # приезд: момент входа в радиус станции
    at_ts: Optional[int] = None         # первое сообщение в состоянии «на станции»
    served: bool = False                # станция уже освобождена по таймеру, машина ещё в радиусе
    in_dump: bool = False               # сейчас в радиусе точки разгрузки
    decided: bool = False               # рекомендация за этот заезд уже считалась


@dataclass
class StationState:
    occupant: Optional[str] = None
    occupied_at: int = 0
    free_since: int = NEG_INF           # когда станция освободилась в последний раз
