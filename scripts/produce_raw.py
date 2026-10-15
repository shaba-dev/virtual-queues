"""Отправляет в Kafka заранее заготовленное «плохое» сообщение — для негативных проверок из TESTING*.md.
Без docker exec, без кавычек и без флагов вроде -T: подходит для Windows PowerShell.

    python scripts/produce_raw.py garbage         # не-JSON в telemetry.v1            (шаг 3.6)
    python scripts/produce_raw.py unknown-unit    # JSON с неизвестной машиной X-1     (шаг 3.6)
    python scripts/produce_raw.py broken-state    # мусор вместо снимка в vqueue.state.v1 (шаг 7б)

Все пресеты можно переопределить: --topic, --key, --value.
"""
import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from vqueue.config import DEFAULT_BOOTSTRAP  # noqa: E402

from confluent_kafka import Producer  # noqa: E402

PRESETS = {
    "garbage": ("telemetry.v1", None, "not json"),
    "unknown-unit": ("telemetry.v1", None,
                     '{"unit_uuid":"X-1","ts":1757930400,"lat":49.1,"lon":142.6,"speed_kmh":10}'),
    "broken-state": ("vqueue.state.v1", "state", "{broken"),
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("preset", choices=sorted(PRESETS))
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--topic")
    ap.add_argument("--key")
    ap.add_argument("--value")
    a = ap.parse_args()
    topic, key, value = PRESETS[a.preset]
    topic, key, value = a.topic or topic, a.key if a.key is not None else key, a.value if a.value is not None else value

    errors = []
    p = Producer({"bootstrap.servers": a.bootstrap})
    p.produce(topic, key=key.encode() if key else None, value=value.encode(),
              on_delivery=lambda err, msg: errors.append(err) if err else None)
    left = p.flush(15)
    if errors or left:
        print(f"НЕ ОТПРАВЛЕНО: {errors[0] if errors else 'таймаут'}")
        return 1
    print(f"отправлено в {topic}: ключ={key!r}, значение={value!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
