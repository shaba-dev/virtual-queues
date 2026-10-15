"""Прогон без брокера: симулятор -> шум -> движок. Для проверки логики и демонстрации.

    python -m vqueue.simulate --config config/site.json --minutes 60 --follow-decisions
"""
from __future__ import annotations

import argparse
import json
from collections import Counter

from .codec import decision_to_dict, iso, parse_telemetry, queue_to_dict
from .config import SiteConfig
from .engine import Engine
from .models import DecisionEvent, QueueEvent
from .sim import Noise, Simulator


def run(cfg: SiteConfig, t0: int, seconds: int, seed: int = 1, dup_rate: float = 0.0,
        late_rate: float = 0.0, follow_decisions: bool = True):
    sim = Simulator(cfg, t0, seed)
    noise = Noise(dup_rate, late_rate, seed=seed + 100)
    eng = Engine(cfg)
    events: list[QueueEvent | DecisionEvent] = []
    for _ in range(seconds):
        batch = noise.push(sim.t + 1, sim.tick())
        for m in batch:
            for ev in eng.process(m):
                events.append(ev)
                if follow_decisions and isinstance(ev, DecisionEvent) and ev.result == "recommended":
                    sim.redirect(ev.unit_uuid, ev.to_station)
    for m in noise.flush():                     # как генератор: в конце отдаёт задержанные сообщения
        events.extend(eng.process(m))
    return sim, eng, events


def replay(cfg: SiteConfig, lines):
    """Прогоняет записанную телеметрию (JSON-строки) через движок: «эталон» для сверки с Kafka."""
    eng = Engine(cfg)
    events: list[QueueEvent | DecisionEvent] = []
    bad = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        t = parse_telemetry(line)
        if t is None:
            bad += 1
            continue
        events.extend(eng.process(t))
    return eng, events, bad


def write_jsonl(path: str, docs) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False, separators=(",", ":")) + "\n")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/site.json")
    ap.add_argument("--minutes", type=int, default=60)
    ap.add_argument("--t0", type=int, default=1757930400)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--dup-rate", type=float, default=0.02)
    ap.add_argument("--late-rate", type=float, default=0.02)
    ap.add_argument("--no-follow-decisions", action="store_true")
    ap.add_argument("--print", dest="show", type=int, default=5, help="сколько примеров вывести")
    ap.add_argument("--replay", metavar="FILE", help="не симулировать, а прогнать телеметрию из JSON-строк")
    ap.add_argument("--queue-out", metavar="FILE", help="записать события queue.v1 в JSON-строки")
    ap.add_argument("--decision-out", metavar="FILE", help="записать события decision.v1 в JSON-строки")
    a = ap.parse_args(argv)
    cfg = SiteConfig.load(a.config)
    if a.replay:
        with open(a.replay, encoding="utf-8") as f:
            eng, events, bad = replay(cfg, f)
        print(f"повторно проиграно из {a.replay}; нечитаемых строк: {bad}")
    else:
        sim, eng, events = run(cfg, a.t0, a.minutes * 60, a.seed, a.dup_rate, a.late_rate,
                               not a.no_follow_decisions)
    qs = [e for e in events if isinstance(e, QueueEvent)]
    ds = [e for e in events if isinstance(e, DecisionEvent)]
    if a.queue_out:
        write_jsonl(a.queue_out, (queue_to_dict(e) for e in qs))
    if a.decision_out:
        write_jsonl(a.decision_out, (decision_to_dict(e) for e in ds))
    print(f"конец симуляции: {iso(eng.now)}; сообщений принято {eng.stats}")
    print(f"queue.v1: {len(qs)} событий; decision.v1: {len(ds)}")
    print("решения:", dict(Counter((d.result, d.reason or d.to_station) for d in ds)))
    for d in [x for x in ds if x.result == "recommended"][: a.show]:
        print(json.dumps(decision_to_dict(d), ensure_ascii=False))
    for sid in cfg.stations:
        print(json.dumps(queue_to_dict(QueueEvent(sid, eng.now, eng.queue(sid))), ensure_ascii=False)[:600])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
