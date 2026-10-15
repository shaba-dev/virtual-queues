"""Воспроизводит пример из задания без Kafka и печатает результат в формате queue.v1 / decision.v1.

    python scripts/check_spec_example.py

Станция S1 в начале координат на меридиане, S2 — в 3300 м севернее (так до T4 ровно 2400 м).
"""
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from vqueue.codec import decision_to_dict, parse_iso, queue_to_dict       # noqa: E402
from vqueue.config import Params, Point, SiteConfig                        # noqa: E402
from vqueue.engine import DecisionEvent, Engine, QueueEvent, Telemetry     # noqa: E402
from vqueue.geo import offset_north                                        # noqa: E402

LAT, LON = 49.1288, 142.6601


def pt(north):
    return Point(*offset_north(LAT, LON, north))


def msg(unit, ts, north, speed):
    p = pt(north)
    return Telemetry(unit, ts, p.lat, p.lon, speed)


def show(title, doc):
    print(f"{title}\n  " + json.dumps(doc, ensure_ascii=False))


def main() -> int:
    t = parse_iso("2026-09-15T12:00:00Z")
    cfg = SiteConfig({"S1": pt(0), "S2": pt(3300)}, pt(-3000),
                     {"T1": "S1", "T2": "S1", "T3": "S1", "T4": "S1"}, Params())
    e = Engine(cfg)
    for m in (msg("T1", t - 60, 15, 0), msg("T1", t, 15, 0), msg("T2", t, 1200, 36), msg("T3", t, 3000, 36)):
        e.process(m)
    show("Очередь S1 в 12:00:00 (ожидаем T1: 0 с, T2: 50 с, T3: 100 с):",
         queue_to_dict(QueueEvent("S1", e.now, e.queue("S1"))))
    events = e.process(msg("T4", t, 900, 36))
    for ev in events:
        if isinstance(ev, DecisionEvent):
            show("Решение для T4 (ожидаем recommended S1 -> S2, gain_seconds 80):", decision_to_dict(ev))
    show("Очередь S2 после решения (ожидаем T4, приезд 12:04:00, ожидание 0):",
         queue_to_dict(QueueEvent("S2", e.now, e.queue("S2"))))
    ok = (e.queue("S1")[2].wait_seconds == 100 and e.queue("S2")[0].eta == t + 240
          and any(isinstance(x, DecisionEvent) and x.gain_seconds == 80 for x in events))
    print("\nРЕЗУЛЬТАТ:", "СОВПАДАЕТ С ПРИМЕРОМ" if ok else "РАСХОДИТСЯ С ПРИМЕРОМ")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
