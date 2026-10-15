"""Создаёт (и при необходимости пересоздаёт) топики. Нужен confluent-kafka и доступный брокер.

    python scripts/create_topics.py                                  # создать недостающие
    python scripts/create_topics.py --recreate --reset-group vqueue-planner
                                                                     # чистый старт: всё удалить и создать
    python scripts/create_topics.py --recreate --only queue.v1,decision.v1,vqueue.state.v1 \
                                    --reset-group vqueue-planner     # потеря состояния: вход оставить
"""
import argparse
import time
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from vqueue.config import DEFAULT_BOOTSTRAP  # noqa: E402

from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

H = 3600 * 1000


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--replication", type=int, default=1)
    ap.add_argument("--telemetry-retention-h", type=int, default=6, help="окно, из которого можно пересобрать состояние")
    ap.add_argument("--recreate", action="store_true", help="сначала удалить топики, если они есть")
    ap.add_argument("--only", default=None, help="через запятую: какие топики трогать (по умолчанию все)")
    ap.add_argument("--reset-group", default=None, metavar="GROUP",
                    help="удалить consumer-группу (сервис должен быть остановлен)")
    a = ap.parse_args()
    rf = a.replication
    topics = [
        # ключ unit_uuid, одна партиция: весь поток площадки в одном порядке (см. ARCHITECTURE.md, п. 4)
        NewTopic("telemetry.v1", 1, rf, config={"retention.ms": str(a.telemetry_retention_h * H)}),
        NewTopic("queue.v1", 4, rf, config={"retention.ms": str(24 * H)}),          # ключ station_uuid
        NewTopic("decision.v1", 4, rf, config={"retention.ms": str(24 * H)}),       # ключ unit_uuid
        # снимок состояния процессора: одна партиция, хранится последнее значение
        NewTopic("vqueue.state.v1", 1, rf, config={"cleanup.policy": "compact", "segment.ms": str(10 * 60 * 1000),
                                                    "min.cleanable.dirty.ratio": "0.1"}),
    ]
    if a.only:
        wanted = set(a.only.split(","))
        topics = [t for t in topics if t.topic in wanted]
    admin = AdminClient({"bootstrap.servers": a.bootstrap})

    if a.reset_group:
        for g, fut in admin.delete_consumer_groups([a.reset_group]).items():
            try:
                fut.result()
                print("группа удалена:", g)
            except Exception as e:
                print(f"группа {g}: {e}")

    if a.recreate:
        for name, fut in admin.delete_topics([t.topic for t in topics], operation_timeout=30).items():
            try:
                fut.result()
                print("удалён", name)
            except Exception as e:
                print(f"{name}: {e}")
        time.sleep(5)                                # брокер удаляет асинхронно

    pending = topics
    for attempt in range(10):
        failed = []
        for t, (name, fut) in zip(pending, admin.create_topics(pending).items()):
            try:
                fut.result()
                print("создан", name)
            except Exception as e:
                if "ALREADY_EXISTS" in str(e) and not a.recreate:
                    print(name, "— уже существует")
                elif "ALREADY_EXISTS" in str(e):
                    failed.append(t)                 # ещё удаляется — повторим
                else:
                    print(name, "—", e)
        if not failed:
            return 0
        pending = failed
        time.sleep(3)
    print("не удалось создать:", [t.topic for t in pending])
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
