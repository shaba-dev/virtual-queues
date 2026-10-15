"""Проверка выходных топиков: формат, инварианты, сверка с «эталоном».

Чистые функции (без Kafka) + CLI. Режимы:

    python -m vqueue.verify check   --queue q.jsonl --decision d.jsonl
    python -m vqueue.verify compare --golden g.jsonl --actual a.jsonl --key station_uuid
    python -m vqueue.verify consume --bootstrap 127.0.0.1:9092 --queue-out q.jsonl --decision-out d.jsonl
    python -m vqueue.verify full    --bootstrap 127.0.0.1:9092 --golden-queue gq.jsonl --golden-decision gd.jsonl
    python -m vqueue.verify state   --bootstrap 127.0.0.1:9092

«Эталон» делается офлайн: `python -m vqueue.simulate --replay telemetry.jsonl --queue-out gq.jsonl ...`.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from typing import Iterable

from .codec import parse_iso
from .config import DEFAULT_BOOTSTRAP, SiteConfig

QUEUE_KEYS = {"station_uuid", "at", "queue"}
ENTRY_KEYS = {"unit_uuid", "eta", "service_start", "free_at", "wait_seconds"}
REJECT_REASONS = {"no_gain", "stale_telemetry"}


def load_jsonl(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _ts(s):
    return None if s is None else parse_iso(s)


def check_queue_doc(doc: dict, cfg: SiteConfig) -> list[str]:
    p, out = cfg.params, []
    if set(doc) != QUEUE_KEYS:
        return [f"queue: неверный набор полей {sorted(doc)}"]
    if doc["station_uuid"] not in cfg.stations:
        out.append(f"queue: неизвестная станция {doc['station_uuid']}")
    try:
        at = _ts(doc["at"])
    except Exception:
        return out + [f"queue: плохое время at={doc['at']!r}"]
    seen, prev_free = set(), None
    for i, e in enumerate(doc["queue"]):
        where = f"queue {doc['station_uuid']}@{doc['at']}[{i}]"
        if set(e) != ENTRY_KEYS:
            out.append(f"{where}: неверный набор полей {sorted(e)}")
            continue
        try:
            eta, start, free = _ts(e["eta"]), _ts(e["service_start"]), _ts(e["free_at"])
        except Exception:
            out.append(f"{where}: плохой формат времени")
            continue
        if e["unit_uuid"] in seen:
            out.append(f"{where}: машина {e['unit_uuid']} дважды")
        seen.add(e["unit_uuid"])
        if e["unit_uuid"] not in cfg.assignments:
            out.append(f"{where}: неизвестная машина {e['unit_uuid']}")
        if free != start + p.slot_s:
            out.append(f"{where}: free_at != service_start + {p.slot_s}")
        if eta is None:
            if i != 0:
                out.append(f"{where}: eta=null не у первой записи")
            if e["wait_seconds"] != 0:
                out.append(f"{where}: у занявшей станцию wait_seconds != 0")
        else:
            if e["wait_seconds"] != start - eta:
                out.append(f"{where}: wait_seconds != service_start - eta")
            if e["wait_seconds"] < 0:
                out.append(f"{where}: отрицательное ожидание")
            if eta > at + p.horizon_s:
                out.append(f"{where}: приезд за горизонтом {p.horizon_s} с")
        if prev_free is not None and start < prev_free:
            out.append(f"{where}: старт раньше освобождения предыдущей")
        prev_free = free
    return out


def check_decision_doc(doc: dict, cfg: SiteConfig) -> list[str]:
    where = f"decision {doc.get('unit_uuid')}@{doc.get('at')}"
    unit = doc.get("unit_uuid")
    if unit not in cfg.assignments:
        return [f"{where}: неизвестная машина"]
    try:
        _ts(doc["at"])
    except Exception:
        return [f"{where}: плохое время at"]
    if doc.get("result") == "recommended":
        if set(doc) != {"unit_uuid", "at", "result", "from_station", "to_station", "gain_seconds"}:
            return [f"{where}: неверный набор полей {sorted(doc)}"]
        out = []
        if doc["from_station"] != cfg.assignments[unit]:
            out.append(f"{where}: from_station не совпадает с закреплённой станцией")
        if doc["to_station"] == doc["from_station"] or doc["to_station"] not in cfg.stations:
            out.append(f"{where}: некорректный to_station")
        if not isinstance(doc["gain_seconds"], int) or doc["gain_seconds"] < cfg.params.min_gain_s:
            out.append(f"{where}: выигрыш {doc['gain_seconds']} < порога {cfg.params.min_gain_s}")
        return out
    if doc.get("result") == "rejected":
        if set(doc) != {"unit_uuid", "at", "result", "reason"}:
            return [f"{where}: неверный набор полей {sorted(doc)}"]
        return [] if doc["reason"] in REJECT_REASONS else [f"{where}: неизвестная причина {doc['reason']}"]
    return [f"{where}: неизвестный result {doc.get('result')!r}"]


def check_streams(queue_docs: Iterable[dict], decision_docs: Iterable[dict], cfg: SiteConfig) -> list[str]:
    """Проверки по отдельным сообщениям + по потоку: монотонность `at`, повторы, дубли решений."""
    out: list[str] = []
    last_at: dict[str, str] = {}
    last_q: dict[str, list] = {}
    for d in queue_docs:
        out += check_queue_doc(d, cfg)
        st = d.get("station_uuid")
        if st in last_at and d.get("at", "") < last_at[st]:
            out.append(f"queue {st}: `at` пошло назад: {last_at[st]} -> {d['at']}")
        last_at[st] = d.get("at", "")
        if st in last_q and last_q[st] == d.get("queue"):
            out.append(f"queue {st}@{d.get('at')}: повтор предыдущей очереди (не должен публиковаться)")
        last_q[st] = d.get("queue")
    seen = set()
    for d in decision_docs:
        out += check_decision_doc(d, cfg)
        k = (d.get("unit_uuid"), d.get("at"))
        if k in seen:
            out.append(f"decision {k}: дубль решения")
        seen.add(k)
    return out


def compare(golden: list[dict], actual: list[dict], key: str) -> list[str]:
    """Сравнивает последовательности сообщений по ключу (порядок внутри ключа должен совпасть)."""
    def group(docs):
        g = defaultdict(list)
        for d in docs:
            g[d[key]].append(d)
        return g
    g, a, out = group(golden), group(actual), []
    for k in sorted(set(g) | set(a)):
        if k not in a:
            out.append(f"{key}={k}: нет в фактическом выходе ({len(g[k])} ожидалось)")
        elif k not in g:
            out.append(f"{key}={k}: лишний в фактическом выходе ({len(a[k])} сообщений)")
        elif g[k] != a[k]:
            n = next((i for i, (x, y) in enumerate(zip(g[k], a[k])) if x != y), min(len(g[k]), len(a[k])))
            out.append(f"{key}={k}: расхождение; ожидалось {len(g[k])}, получено {len(a[k])}, "
                       f"первое отличие на позиции {n}:\n    эталон : {g[k][n] if n < len(g[k]) else '—'}"
                       f"\n    факт   : {a[k][n] if n < len(a[k]) else '—'}")
    return out


# ------------------------------------------------------------------ Kafka

def consume_topics(bootstrap: str, topics: list[str], idle_s: float = 10.0, grace_s: float = 25.0):
    """Читает топики с начала (read_committed) до паузы `idle_s` без новых сообщений."""
    from confluent_kafka import Consumer, KafkaError
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"vqueue-verify-{time.time_ns()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False,
                  "isolation.level": "read_committed"})
    c.subscribe(topics)
    docs: dict[str, list[dict]] = {t: [] for t in topics}
    t0 = last = time.monotonic()
    got = False
    try:
        while True:
            limit = idle_s if got else max(idle_s, grace_s)
            if time.monotonic() - last > limit:
                break
            m = c.poll(1.0)
            if m is None:
                continue
            if m.error():
                if m.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                raise RuntimeError(m.error())
            docs[m.topic()].append(json.loads(m.value()))
            got, last = True, time.monotonic()
    finally:
        c.close()
    return docs


def read_state(bootstrap: str, topic: str = "vqueue.state.v1"):
    """Последний снимок состояния из Kafka (тот же код чтения, что и у сервиса)."""
    from .kafka_io import read_last_value
    raw = read_last_value(bootstrap, topic, b"state", f"vqueue-verify-{time.time_ns()}")
    return None if raw is None else json.loads(raw)


def _write(path, docs):
    with open(path, "w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False, separators=(",", ":")) + "\n")


def _report(title: str, problems: list[str]) -> bool:
    if problems:
        print(f"FAIL  {title}: {len(problems)} проблем(ы)")
        for p in problems[:10]:
            print("   -", p)
        if len(problems) > 10:
            print(f"   … и ещё {len(problems) - 10}")
        return False
    print(f"OK    {title}")
    return True


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Проверка queue.v1 / decision.v1")
    ap.add_argument("mode", choices=["check", "compare", "consume", "full", "state"])
    ap.add_argument("--config", default="config/site.json")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--queue", help="check: файл с queue.v1")
    ap.add_argument("--decision", help="check: файл с decision.v1")
    ap.add_argument("--golden", help="compare: эталон")
    ap.add_argument("--actual", help="compare: факт")
    ap.add_argument("--key", default="station_uuid")
    ap.add_argument("--dedup-actual", action="store_true",
                    help="compare: перед сверкой убрать из факта точные повторы сообщений")
    ap.add_argument("--queue-out", default="actual_queue.jsonl")
    ap.add_argument("--decision-out", default="actual_decision.jsonl")
    ap.add_argument("--golden-queue")
    ap.add_argument("--golden-decision")
    ap.add_argument("--idle", type=float, default=10.0, help="сколько секунд тишины считать концом потока")
    a = ap.parse_args(argv)
    cfg = SiteConfig.load(a.config)

    if a.mode == "state":
        st = read_state(a.bootstrap)
        if st is None:
            print("снимка состояния в vqueue.state.v1 нет")
            return 1
        from .codec import iso
        eng = st["engine"]
        print(f"оффсеты входа: {st['offsets']}")
        print(f"«сейчас» движка: {iso(eng['now'])}; машин в состоянии: {len(eng['units'])}")
        print(f"счётчики движка: {eng['stats']}")
        return 0

    if a.mode == "check":
        q = load_jsonl(a.queue) if a.queue else []
        d = load_jsonl(a.decision) if a.decision else []
        ok = _report(f"формат и инварианты ({len(q)} queue, {len(d)} decision)", check_streams(q, d, cfg))
        return 0 if ok else 1

    if a.mode == "compare":
        actual = load_jsonl(a.actual)
        if a.dedup_actual:
            seen, uniq = set(), []
            for d in actual:
                k = json.dumps(d, sort_keys=True)
                if k not in seen:
                    seen.add(k)
                    uniq.append(d)
            print(f"повторов убрано: {len(actual) - len(uniq)}")
            actual = uniq
        ok = _report(f"{a.actual} совпадает с эталоном {a.golden} (по {a.key})",
                     compare(load_jsonl(a.golden), actual, a.key))
        return 0 if ok else 1

    docs = consume_topics(a.bootstrap, ["queue.v1", "decision.v1"], a.idle)
    q, d = docs["queue.v1"], docs["decision.v1"]
    _write(a.queue_out, q)
    _write(a.decision_out, d)
    print(f"прочитано: queue.v1 — {len(q)}, decision.v1 — {len(d)} (записано в {a.queue_out}, {a.decision_out})")
    if a.mode == "consume":
        return 0
    if not q and not d:
        print("FAIL  выходные топики пусты — сверять нечего. Вероятные причины:")
        print("   - сервис не запущен (или упал) — проверьте окно сервиса и запустите его;")
        print("   - сервис ещё не успел: дождитесь LAG = 0 "
              "(kafka-consumer-groups.sh --describe --group vqueue-planner);")
        print("   - в telemetry.v1 нет данных (kafka-get-offsets.sh --topic telemetry.v1) — "
              "не запускали генератор или писали не в те топики;")
        print("   - топики пересоздали после запуска сервиса — остановите сервис и запустите заново.")
        print("ИТОГ: ЕСТЬ ОШИБКИ")
        return 1
    ok = _report("формат и инварианты", check_streams(q, d, cfg))
    if a.golden_queue:
        g = load_jsonl(a.golden_queue)
        ok &= _report(f"queue.v1 совпадает с эталоном ({len(g)} сообщений)", compare(g, q, "station_uuid"))
    if a.golden_decision:
        g = load_jsonl(a.golden_decision)
        ok &= _report(f"decision.v1 совпадает с эталоном ({len(g)} сообщений)", compare(g, d, "unit_uuid"))
    print("ИТОГ:", "ВСЁ ПРОШЛО" if ok else "ЕСТЬ ОШИБКИ")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
