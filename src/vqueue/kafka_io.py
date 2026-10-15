"""Kafka-транспорт на confluent-kafka: consume-process-produce в транзакции.

Один коммит = выходные сообщения + снимок состояния + оффсеты входа (exactly-once
между топиками). Состояние восстанавливается из компактного топика состояния.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from confluent_kafka import (OFFSET_BEGINNING, Consumer, KafkaError, KafkaException,
                             Producer, TopicPartition)

from .config import DEFAULT_BOOTSTRAP
from .runner import REPLAY_FROM_START, STATE_KEY, FatalTransportError, InMsg, OutMsg, RestoreFn, Topics

log = logging.getLogger("vqueue.kafka")


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap: str = DEFAULT_BOOTSTRAP
    group_id: str = "vqueue-planner"
    transactional_id: str = "vqueue-planner"     # один на группу: новый активный экземпляр вытесняет зомби
    topics: Topics = Topics()
    auto_offset_reset: str = "earliest"
    transaction_timeout_ms: int = 60_000


def read_last_value(bootstrap: str, topic: str, key: bytes, group_id: str) -> Optional[bytes]:
    """Последнее значение ключа `key` в одно-партиционном топике (read_committed); None, если записей нет.

    Единственная реализация чтения снимка состояния: её используют и сервис при старте, и `verify state`.
    """
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "enable.partition.eof": True,
    })
    last = None
    try:
        c.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
        while True:
            m = c.poll(10.0)
            if m is None:
                log.warning("таймаут чтения топика %s", topic)
                break
            err = m.error()
            if err:
                if err.code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    break
                raise KafkaException(err)
            if m.key() == key:
                last = m.value()
    finally:
        c.close()
    return last


class KafkaTransport:
    def __init__(self, s: KafkaSettings):
        self.s = s
        self.consumer = Consumer({
            "bootstrap.servers": s.bootstrap,
            "group.id": s.group_id,
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "auto.offset.reset": s.auto_offset_reset,
            "enable.partition.eof": False,
        })
        # Продюсер создаётся при каждой выдаче партиций (см. _on_assign), а не здесь: init_transactions()
        # на одном продюсере можно вызвать только один раз.
        self.producer: Optional[Producer] = None
        self._restore: Optional[RestoreFn] = None

    def _make_producer(self) -> Producer:
        s = self.s
        return Producer({
            "bootstrap.servers": s.bootstrap,
            "transactional.id": s.transactional_id,
            "enable.idempotence": True,
            "acks": "all",
            "transaction.timeout.ms": s.transaction_timeout_ms,
        })

    # --- старт и ребаланс

    def start(self, restore: RestoreFn) -> None:
        self._restore = restore
        self.consumer.subscribe([self.s.topics.telemetry], on_assign=self._on_assign,
                                on_revoke=self._on_revoke)

    def _on_assign(self, consumer, partitions) -> None:
        if not partitions:
            # Резервный экземпляр: ему ничего не досталось (партиция одна, её держит другой). Нельзя вызывать
            # init_transactions() — тот же transactional.id вытеснил бы активного владельца.
            log.info("партиции не получены — резервный режим")
            consumer.assign(partitions)
            return
        # Выдача партиций повторяется при каждом ребалансе (вошёл или вышел другой экземпляр, перезапуск
        # брокера...). Поэтому каждый раз берём НОВЫЙ продюсер: повторный init_transactions() на старом даёт
        # «Operation not valid in state Ready». Новая инициализация с тем же transactional.id заодно
        # вытесняет прежнего владельца (зомби) и закрывает его незавершённую транзакцию.
        # Порядок важен: сначала вытесняем, потом читаем снимок — иначе зомби успеет записать ещё один.
        self.producer = self._make_producer()
        self.producer.init_transactions()
        offsets = self._restore(self._read_snapshot())
        for p in partitions:
            if offsets == REPLAY_FROM_START:
                p.offset = OFFSET_BEGINNING
            elif offsets and p.partition in offsets:
                p.offset = offsets[p.partition]
        consumer.assign(partitions)

    def _on_revoke(self, consumer, partitions) -> None:
        log.info("партиции отозваны: %s", [p.partition for p in partitions])

    def _read_snapshot(self) -> Optional[bytes]:
        """Последнее значение ключа state из топика состояния (одна партиция, compact)."""
        return read_last_value(self.s.bootstrap, self.s.topics.state, STATE_KEY,
                               f"{self.s.group_id}-state-reader")

    # --- чтение / коммит

    def poll(self, max_messages: int, timeout: float) -> list[InMsg]:
        out = []
        for m in self.consumer.consume(num_messages=max_messages, timeout=timeout):
            err = m.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(err)
            out.append(InMsg(m.partition(), m.offset(), m.key(), m.value()))
        return out

    def _produce(self, topic: str, key: bytes, value: bytes) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key, value=value)
                return
            except BufferError:
                self.producer.poll(0.1)

    def commit(self, outputs: list[OutMsg], snapshot: bytes, offsets: dict[int, int]) -> None:
        self.producer.begin_transaction()
        try:
            for o in outputs:
                self._produce(o.topic, o.key, o.value)
            self._produce(self.s.topics.state, STATE_KEY, snapshot)
            self.producer.send_offsets_to_transaction(
                [TopicPartition(self.s.topics.telemetry, p, o) for p, o in offsets.items()],
                self.consumer.consumer_group_metadata(),
            )
            self.producer.commit_transaction()
        except KafkaException as e:
            err = e.args[0]
            if err.fatal() or err.code() == KafkaError._FENCED:
                raise FatalTransportError(str(err)) from e
            self.producer.abort_transaction()
            raise

    def rollback(self, start_offsets: dict[int, int]) -> None:
        for p, off in start_offsets.items():
            self.consumer.seek(TopicPartition(self.s.topics.telemetry, p, off))

    def close(self) -> None:
        self.consumer.close()
        if self.producer is not None:
            self.producer.flush(10)
