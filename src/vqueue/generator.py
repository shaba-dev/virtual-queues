"""Генератор телеметрии: симулятор площадки -> topic telemetry.v1.

    python -m vqueue.generator --bootstrap 127.0.0.1:9092 --speedup 60 --dup-rate 0.02 --late-rate 0.02
    python -m vqueue.generator --stdout --duration 600          # без Kafka: JSON-строки в stdout
    python -m vqueue.generator --out work/telemetry.jsonl ...   # без Kafka: JSON-строки в файл

--speedup N: симулируемая секунда занимает 1/N реальной (1 — реальный темп, 0 — без пауз).
--follow-decisions: водители выполняют рекомендации из decision.v1 (замкнутый контур).
"""
from __future__ import annotations

import argparse
import itertools
import json
import logging
import sys
import time

from .codec import dumps, telemetry_to_dict
from .config import DEFAULT_BOOTSTRAP, SiteConfig
from .sim import Noise, Simulator

log = logging.getLogger("vqueue.generator")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Генератор телеметрии telemetry.v1")
    ap.add_argument("--config", default="config/site.json")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--topic", default="telemetry.v1")
    ap.add_argument("--decision-topic", default="decision.v1")
    ap.add_argument("--speedup", type=float, default=1.0)
    ap.add_argument("--duration", type=int, default=0, help="сколько секунд симулировать (0 — бесконечно)")
    ap.add_argument("--start-ts", type=int, default=None, help="начало, epoch-секунды (по умолчанию — сейчас)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--dup-rate", type=float, default=0.01)
    ap.add_argument("--late-rate", type=float, default=0.02)
    ap.add_argument("--max-late", type=int, default=60)
    ap.add_argument("--follow-decisions", action="store_true")
    ap.add_argument("--stdout", action="store_true", help="печатать в stdout вместо Kafka")
    ap.add_argument("--out", metavar="FILE", help="писать JSON-строки в файл (UTF-8) вместо Kafka; "
                                                  "надёжнее, чем перенаправление `>` в Windows PowerShell")
    ap.add_argument("--log-level", default="INFO")
    a = ap.parse_args(argv)
    logging.basicConfig(level=a.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = SiteConfig.load(a.config)
    t0 = a.start_ts if a.start_ts is not None else int(time.time())
    sim = Simulator(cfg, t0, a.seed)
    noise = Noise(a.dup_rate, a.late_rate, a.max_late, seed=a.seed + 100)

    producer = consumer = None
    sink = open(a.out, "w", encoding="utf-8", newline="\n") if a.out else sys.stdout
    if not (a.stdout or a.out):
        from confluent_kafka import Consumer, Producer          # нужен confluent-kafka
        producer = Producer({"bootstrap.servers": a.bootstrap, "enable.idempotence": True,
                             "acks": "all", "linger.ms": 5})
        if a.follow_decisions:
            consumer = Consumer({"bootstrap.servers": a.bootstrap, "group.id": f"generator-{time.time_ns()}",
                                 "auto.offset.reset": "latest", "isolation.level": "read_committed",
                                 "enable.auto.commit": False})
            consumer.subscribe([a.decision_topic])

    def on_delivery(err, _msg):
        if err is not None:
            log.error("доставка не удалась: %s", err)

    def send(msgs):
        for m in msgs:
            value = dumps(telemetry_to_dict(m))
            if producer is None:
                sink.write(value.decode() + "\n")
            else:
                while True:
                    try:
                        producer.produce(a.topic, key=m.unit_uuid.encode(), value=value, on_delivery=on_delivery)
                        break
                    except BufferError:
                        producer.poll(0.1)
        if producer is not None:
            producer.poll(0)

    def apply_decisions():
        while consumer is not None:
            m = consumer.poll(0)
            if m is None:
                return
            if m.error():
                continue
            d = json.loads(m.value())
            if d.get("result") == "recommended":
                sim.redirect(d["unit_uuid"], d["to_station"])

    wall0 = time.monotonic()
    try:
        for i in itertools.count():
            if a.duration and i >= a.duration:
                break
            if a.speedup > 0:
                delay = wall0 + i / a.speedup - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            apply_decisions()
            send(noise.push(sim.t + 1, sim.tick()))
        send(noise.flush())
    except KeyboardInterrupt:
        pass
    finally:
        if producer is not None:
            producer.flush(10)
        if consumer is not None:
            consumer.close()
        if sink is not sys.stdout:
            sink.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
