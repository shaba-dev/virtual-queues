"""Точка входа сервиса: python -m vqueue.service --config config/site.json"""
from __future__ import annotations

import argparse
import logging
import signal
import time

from .config import DEFAULT_BOOTSTRAP, SiteConfig
from .runner import Runner, Topics


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Сервис виртуальных очередей (Kafka -> Kafka)")
    ap.add_argument("--config", default="config/site.json")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--group", default="vqueue-planner")
    ap.add_argument("--transactional-id", default=None, help="по умолчанию = --group")
    ap.add_argument("--max-batch", type=int, default=500)
    ap.add_argument("--offset-reset", default="earliest", choices=["earliest", "latest"])
    ap.add_argument("--log-level", default="INFO")
    a = ap.parse_args(argv)
    logging.basicConfig(level=a.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from .kafka_io import KafkaSettings, KafkaTransport      # импорт здесь: нужен confluent-kafka

    cfg = SiteConfig.load(a.config)
    transport = KafkaTransport(KafkaSettings(
        bootstrap=a.bootstrap, group_id=a.group, transactional_id=a.transactional_id or a.group,
        topics=Topics(), auto_offset_reset=a.offset_reset))
    runner = Runner(cfg, transport, max_batch=a.max_batch)

    stop = {"flag": False}
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.update(flag=True))

    runner.start()
    last_log = time.monotonic()
    try:
        while not stop["flag"]:
            runner.step(timeout=0.2)
            if time.monotonic() - last_log > 15:
                last_log = time.monotonic()
                logging.getLogger("vqueue").info("stats=%s engine=%s now=%s", runner.stats,
                                                 runner.engine.stats, runner.engine.now)
    finally:
        transport.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
