"""Ядро расчёта виртуальных очередей.

Чистая логика без Kafka, без часов компьютера и без ввода-вывода: на вход идут
сообщения телеметрии, на выход — события `QueueEvent` и `DecisionEvent`.
Всё время — время событий (секунды epoch, целые). Один и тот же вход всегда
даёт один и тот же выход.

Модели данных — в `models.py`, формат на проводе — в `codec.py`.
"""
from __future__ import annotations

import math
from dataclasses import asdict, fields
from typing import Optional

from .config import SiteConfig
from .geo import haversine_m

from .models import (AT_STATION, NEG_INF, SNAPSHOT_VERSION, TO_DUMP, TO_STATION, DecisionEvent,
                     QueueEntry, QueueEvent, StationState, Telemetry, UnitState)

__all__ = ["Engine", "Telemetry", "QueueEntry", "QueueEvent", "DecisionEvent", "UnitState", "StationState",
           "TO_STATION", "TO_DUMP", "AT_STATION"]   # модели реэкспортируются для удобства импорта из engine


# ----------------------------------------------------------------- движок


class Engine:
    def __init__(self, cfg: SiteConfig):
        self.cfg = cfg
        self.p = cfg.params
        self.units: dict[str, UnitState] = {}
        self.stations: dict[str, StationState] = {s: StationState() for s in cfg.stations}
        self.now: Optional[int] = None
        self._published: dict[str, tuple[QueueEntry, ...]] = {s: () for s in cfg.stations}
        self.stats = {"accepted": 0, "ignored_old": 0, "unknown_unit": 0}
        # путь «разгрузка → станция» для расчёта приезда машины «к разгрузке»
        self._dump_to = {
            sid: haversine_m(cfg.dump.lat, cfg.dump.lon, pt.lat, pt.lon)
            for sid, pt in cfg.stations.items()
        }

    # ---------------------------------------------------------- публичное API

    def process(self, msg: Telemetry) -> list[QueueEvent | DecisionEvent]:
        home = self.cfg.assignments.get(msg.unit_uuid)
        if home is None:
            self.stats["unknown_unit"] += 1
            return []
        u = self.units.get(msg.unit_uuid)
        if u is not None and u.last_ts is not None and msg.ts <= u.last_ts:
            # дубль или нарушение порядка: ts не новее последнего учтённого
            self.stats["ignored_old"] += 1
            return []
        if u is None:
            u = self.units[msg.unit_uuid] = UnitState(msg.unit_uuid, home, home)

        self.stats["accepted"] += 1
        self.now = msg.ts if self.now is None else max(self.now, msg.ts)
        self._update_unit(u, msg)
        self._settle(self.now)

        out: list[QueueEvent | DecisionEvent] = []
        decision = self._maybe_decide(u, msg)
        if decision is not None:
            out.append(decision)
        out.extend(self._publish())
        return out

    def queue(self, station: str) -> tuple[QueueEntry, ...]:
        """Текущая очередь станции на момент «сейчас»."""
        if self.now is None:
            return ()
        return tuple(self._queue(station, self.now))

    # ------------------------------------------------------- снимок состояния

    def snapshot(self) -> dict:
        return {
            "version": SNAPSHOT_VERSION,
            "now": self.now,
            "units": {k: asdict(v) for k, v in self.units.items()},
            "stations": {k: asdict(v) for k, v in self.stations.items()},
            "published": {k: [asdict(e) for e in v] for k, v in self._published.items()},
            "stats": dict(self.stats),
        }

    @classmethod
    def restore(cls, cfg: SiteConfig, snap: dict) -> "Engine":
        if snap.get("version") != SNAPSHOT_VERSION:
            raise ValueError(f"неподдерживаемая версия снимка: {snap.get('version')}")
        e = cls(cfg)
        e.now = snap["now"]
        uf = {f.name for f in fields(UnitState)}
        for k, v in snap["units"].items():
            if k in cfg.assignments:
                e.units[k] = UnitState(**{a: b for a, b in v.items() if a in uf})
        for k, v in snap["stations"].items():
            if k in e.stations:
                e.stations[k] = StationState(**v)
        for k, v in snap["published"].items():
            if k in e._published:
                e._published[k] = tuple(QueueEntry(**x) for x in v)
        e.stats.update(snap.get("stats", {}))
        return e

    # ------------------------------------------------ состояние одной машины

    def _secs(self, meters: float) -> int:
        return int(math.floor(meters / self.p.speed_ms + 0.5))

    @staticmethod
    def _distance(u: UnitState, point) -> float:
        """Расстояние от последней позиции машины до точки (станции или разгрузки)."""
        return haversine_m(u.lat, u.lon, point.lat, point.lon)

    def _arrival(self, u: UnitState, meters: float) -> int:
        """Время приезда: момент позиции + путь на расчётной скорости. Единственное место, где считается ETA."""
        return u.last_ts + self._secs(meters)

    def _update_unit(self, u: UnitState, m: Telemetry) -> None:
        p = self.p
        u.last_ts, u.lat, u.lon = m.ts, m.lat, m.lon

        near = None
        best = p.radius_m
        for sid, pt in self.cfg.stations.items():
            d = haversine_m(m.lat, m.lon, pt.lat, pt.lon)
            if d < best:
                near, best = sid, d
        if near is None:
            u.near = u.near_since = None
        elif u.near != near:
            u.near, u.near_since = near, m.ts

        d_dump = haversine_m(m.lat, m.lon, self.cfg.dump.lat, self.cfg.dump.lon)

        if u.phase == AT_STATION:
            if u.near != u.station:                 # вышла из радиуса станции
                self._leave(u, m.ts, d_dump)
        elif u.phase == TO_DUMP:
            if d_dump < p.radius_m:
                u.in_dump = True
            elif u.in_dump:                         # вышла из радиуса точки разгрузки
                u.in_dump = False
                u.phase = TO_STATION

        if u.phase == TO_STATION and u.near is not None and m.speed_kmh < p.stand_speed_kmh:
            self._enter(u, m.ts)

    def _enter(self, u: UnitState, ts: int) -> None:
        u.phase = AT_STATION
        u.station = u.target = u.near
        u.arrival_ts = u.near_since if u.near_since is not None else ts
        u.at_ts = ts
        u.served = False
        u.decided = False                           # «побывала на станции»

    def _leave(self, u: UnitState, ts: int, d_dump: float) -> None:
        st = self.stations[u.station]
        if st.occupant == u.unit:
            # раньше из двух: 230 с после занятия или выход из радиуса
            st.free_since = min(st.occupied_at + self.p.slot_s, ts)
            st.occupant = None
        u.phase = TO_DUMP
        u.target = u.home                           # следующий заезд — к своей станции
        u.station = u.arrival_ts = u.at_ts = None
        u.served = False
        u.in_dump = d_dump < self.p.radius_m

    def _fresh(self, u: UnitState, now: int) -> bool:
        return u.last_ts is not None and now - u.last_ts <= self.p.fresh_s

    # ------------------------------------------------- занятие / освобождение

    def _settle(self, now: int) -> None:
        """Применяет занятие и освобождение станций до момента `now` (ленивo, по событиям)."""
        slot = self.p.slot_s
        for sid, st in self.stations.items():
            while True:
                if st.occupant is not None:
                    rel = st.occupied_at + slot
                    if rel > now:
                        break
                    occ = self.units.get(st.occupant)
                    if occ is not None:
                        occ.served = True           # не должна занять станцию повторно
                    st.occupant = None
                    st.free_since = rel
                    continue
                waiters = [
                    w for w in self.units.values()
                    if w.phase == AT_STATION and w.station == sid
                    and not w.served and self._fresh(w, now)
                ]
                if not waiters:
                    break
                w = min(waiters, key=lambda x: (x.arrival_ts, x.at_ts, x.unit))
                st.occupant = w.unit
                st.occupied_at = max(w.at_ts, st.free_since)

    # ----------------------------------------------------------------- очередь

    def _queue(self, sid: str, now: int) -> list[QueueEntry]:
        p, st = self.p, self.stations[sid]
        entries: list[QueueEntry] = []
        t_free = NEG_INF
        if st.occupant is not None:
            start = st.occupied_at
            t_free = start + p.slot_s
            entries.append(QueueEntry(st.occupant, None, start, t_free, 0))

        cands: list[tuple[int, str]] = []
        for u in self.units.values():
            if u.unit == st.occupant or u.target != sid or not self._fresh(u, now):
                continue
            if u.phase == AT_STATION:
                if u.station != sid or u.served:
                    continue
                eta = u.arrival_ts
            elif u.phase == TO_STATION:
                eta = self._arrival(u, self._distance(u, self.cfg.stations[sid]))
            else:  # TO_DUMP: до точки разгрузки и от неё до станции
                eta = self._arrival(u, self._distance(u, self.cfg.dump) + self._dump_to[sid])
            if eta > now + p.horizon_s:
                continue
            cands.append((eta, u.unit))

        cands.sort()
        for eta, unit in cands:
            start = max(eta, t_free)
            t_free = start + p.slot_s
            entries.append(QueueEntry(unit, eta, start, t_free, start - eta))
        return entries

    def _publish(self) -> list[QueueEvent]:
        out = []
        for sid in self.cfg.stations:
            q = tuple(self._queue(sid, self.now))
            if q != self._published[sid]:
                self._published[sid] = q
                out.append(QueueEvent(sid, self.now, q))
        return out

    # ------------------------------------------------------------ рекомендация

    def _maybe_decide(self, u: UnitState, m: Telemetry) -> Optional[DecisionEvent]:
        p = self.p
        if u.phase != TO_STATION or u.decided or u.target != u.home:
            return None
        if self._distance(u, self.cfg.stations[u.home]) >= p.decision_radius_m:
            return None
        u.decided = True                            # один расчёт за заезд, в т.ч. отказ
        now = self.now
        if now - m.ts > p.fresh_s:
            return DecisionEvent(u.unit, now, "rejected", reason="stale_telemetry")

        waits: dict[str, int] = {}
        dist: dict[str, float] = {}
        for sid, pt in self.cfg.stations.items():
            d = self._distance(u, pt)
            eta = self._arrival(u, d)
            last_free = None
            for e in self._queue(sid, now):         # очередь отсортирована по порядку обслуживания
                if e.unit_uuid == u.unit:
                    continue
                if e.eta is None or (e.eta, e.unit_uuid) < (eta, u.unit):
                    last_free = e.free_at
            waits[sid] = 0 if last_free is None else max(0, last_free - eta)
            dist[sid] = d

        # меньше ожидание → своя станция → ближайшая
        best = min(waits, key=lambda s: (waits[s], s != u.home, dist[s], s))
        gain = waits[u.home] - waits[best]
        if best == u.home or gain < p.min_gain_s:
            return DecisionEvent(u.unit, now, "rejected", reason="no_gain")
        u.target = best
        return DecisionEvent(u.unit, now, "recommended", u.home, best, gain)
