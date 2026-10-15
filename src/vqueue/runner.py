"""Цикл обработки: пакет сообщений -> движок -> один атомарный коммит.

Транспорт абстрактный (`Transport`): в продакшене это Kafka (`kafka_io.KafkaTransport`),
в тестах — in-memory брокер. За один коммит атомарно уходят: выходные сообщения,
снимок состояния и сдвиг оффсетов входа. Поэтому после сбоя состояние и оффсеты
всегда согласованы, а уже выданные решения не повторяются.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Callable, Optional, Protocol

from .codec import decision_to_dict, dumps, parse_telemetry, queue_to_dict
from .config import SiteConfig
from .engine import Engine
from .models import DecisionEvent, QueueEvent

log = logging.getLogger("vqueue.runner")

REPLAY_FROM_START = "replay_from_start"     # снимок повреждён: перечитать вход с начала
STATE_KEY = b"state"


@dataclass(frozen=True)
class InMsg:
    partition: int
    offset: int
    key: Optional[bytes]
    value: Optional[bytes]


@dataclass(frozen=True)
class OutMsg:
    topic: str
    key: bytes
    value: bytes


@dataclass(frozen=True)
class Topics:
    telemetry: str = "telemetry.v1"
    queue: str = "queue.v1"
    decision: str = "decision.v1"
    state: str = "vqueue.state.v1"


class FatalTransportError(Exception):
    """Ошибка, после которой продолжать нельзя (например, нас вытеснил новый экземпляр)."""


RestoreFn = Callable[[Optional[bytes]], "dict[int, int] | str | None"]


class Transport(Protocol):
    def start(self, restore: RestoreFn) -> None: ...
    def poll(self, max_messages: int, timeout: float) -> list[InMsg]: ...
    def commit(self, outputs: list[OutMsg], snapshot: bytes, offsets: dict[int, int]) -> None: ...
    def rollback(self, start_offsets: dict[int, int]) -> None: ...
    def close(self) -> None: ...


class Runner:
    def __init__(self, cfg: SiteConfig, transport: Transport, topics: Topics = Topics(),
                 max_batch: int = 500, max_failures: int = 5):
        self.cfg, self.transport, self.topics = cfg, transport, topics
        self.max_batch, self.max_failures = max_batch, max_failures
        self.engine = Engine(cfg)
        self.offsets: dict[int, int] = {}
        self.stats = {"batches": 0, "messages": 0, "invalid": 0, "failed_commits": 0,
                      "queue_events": 0, "decisions": 0}
        self._failures = 0
        self._committed: dict = self.engine.snapshot()

    def start(self) -> None:
        self.transport.start(self._restore)

    def _restore(self, raw: Optional[bytes]):
        """Вызывается транспортом при получении партиций: поднимаем состояние из снимка."""
        if raw is None:
            log.info("снимка состояния нет — старт с чистого состояния")
            self.engine, self.offsets = Engine(self.cfg), {}
            self._committed = self.engine.snapshot()
            return None
        try:
            doc = json.loads(raw)
            self.engine = Engine.restore(self.cfg, doc["engine"])
            self.offsets = {int(k): int(v) for k, v in doc["offsets"].items()}
            self._committed = doc["engine"]
            log.info("состояние восстановлено из снимка, оффсеты %s", self.offsets)
            return dict(self.offsets)
        except Exception:
            log.exception("снимок повреждён — перечитываем вход с начала")
            self.engine, self.offsets = Engine(self.cfg), {}
            self._committed = self.engine.snapshot()
            return REPLAY_FROM_START

    def _encode(self, ev: QueueEvent | DecisionEvent) -> OutMsg:
        if isinstance(ev, QueueEvent):
            self.stats["queue_events"] += 1
            return OutMsg(self.topics.queue, ev.station_uuid.encode(), dumps(queue_to_dict(ev)))
        self.stats["decisions"] += 1
        return OutMsg(self.topics.decision, ev.unit_uuid.encode(), dumps(decision_to_dict(ev)))

    def step(self, timeout: float = 1.0) -> int:
        """Один пакет. Возвращает число обработанных входных сообщений."""
        batch = self.transport.poll(self.max_batch, timeout)
        if not batch:
            return 0
        outputs: list[OutMsg] = []
        new_offsets = dict(self.offsets)
        start: dict[int, int] = {}
        invalid = 0
        for m in batch:
            start.setdefault(m.partition, m.offset)
            new_offsets[m.partition] = m.offset + 1
            t = parse_telemetry(m.value) if m.value is not None else None
            if t is None:
                invalid += 1
                continue
            outputs.extend(self._encode(ev) for ev in self.engine.process(t))

        snap_engine = self.engine.snapshot()
        snapshot = dumps({"v": 1, "offsets": {str(k): v for k, v in new_offsets.items()},
                          "engine": snap_engine})
        try:
            self.transport.commit(outputs, snapshot, new_offsets)
        except FatalTransportError:
            raise
        except Exception:
            # коммит не состоялся: откатываем память и перечитываем пакет
            log.exception("коммит не удался, откат пакета")
            self.engine = Engine.restore(self.cfg, self._committed)
            self.transport.rollback(start)
            self.stats["failed_commits"] += 1
            self._failures += 1
            if self._failures >= self.max_failures:
                raise FatalTransportError("слишком много неудачных коммитов подряд")
            return 0
        self._failures = 0
        self._committed, self.offsets = snap_engine, new_offsets
        self.stats["batches"] += 1
        self.stats["messages"] += len(batch)
        self.stats["invalid"] += invalid
        return len(batch)
