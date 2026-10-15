"""Клей Kafka (`kafka_io.py`) против заглушки confluent_kafka.

Это НЕ проверка на живом брокере: здесь проверяется порядок вызовов (init_transactions до чтения
снимка, транзакция вокруг выходов+снимка+оффсетов, abort при сбое, seek при откате) и отсутствие
опечаток. Настоящая проверка — сценарий в TESTING.md.
"""
import importlib
import json
import logging
import sys
import types
import unittest
from unittest import mock

from vqueue.codec import dumps, telemetry_to_dict
from vqueue.config import SiteConfig
from vqueue.engine import Telemetry
from vqueue.runner import FatalTransportError, Runner

logging.disable(logging.CRITICAL)
CFG = SiteConfig.load("config/site.json")


def make_stub():
    calls = []
    m = types.ModuleType("confluent_kafka")
    m.OFFSET_BEGINNING = -2

    class KafkaError:
        _PARTITION_EOF = -191
        UNKNOWN_TOPIC_OR_PART = 3
        _FENCED = -144

    class Err:
        def __init__(self, code=1, fatal=False):
            self._c, self._f = code, fatal
        def code(self): return self._c
        def fatal(self): return self._f

    class KafkaException(Exception):
        pass

    class TopicPartition:
        def __init__(self, topic, partition=-1, offset=-1001):
            self.topic, self.partition, self.offset = topic, partition, offset
        def __repr__(self): return f"TP({self.topic},{self.partition},{self.offset})"

    class Msg:
        def __init__(self, topic, partition, offset, key, value):
            self._a = (topic, partition, offset, key, value)
        def error(self): return None
        def topic(self): return self._a[0]
        def partition(self): return self._a[1]
        def offset(self): return self._a[2]
        def key(self): return self._a[3]
        def value(self): return self._a[4]

    state = {"inbox": [], "snapshot": None, "fail": None, "readers": 0}

    class Consumer:
        def __init__(self, conf):
            self.conf, self.is_reader = conf, "state-reader" in conf.get("group.id", "")
            self._snap_sent = False
            calls.append(("Consumer", conf.get("group.id")))
        def subscribe(self, topics, on_assign=None, on_revoke=None):
            calls.append(("subscribe", tuple(topics)))
            self.on_assign = on_assign
        def assign(self, parts): calls.append(("assign", [(p.topic, p.partition, p.offset) for p in parts]))
        def consume(self, num_messages, timeout):
            out, state["inbox"] = state["inbox"][:num_messages], state["inbox"][num_messages:]
            return out
        def poll(self, timeout):                       # чтение топика состояния
            if state["snapshot"] is not None and not self._snap_sent:
                self._snap_sent = True
                return Msg("vqueue.state.v1", 0, 0, b"state", state["snapshot"])
            class Eof:
                def error(self_): return Err(KafkaError._PARTITION_EOF)
            return Eof()
        def seek(self, tp): calls.append(("seek", tp.partition, tp.offset))
        def consumer_group_metadata(self): return "GROUP_META"
        def close(self): calls.append(("close",))

    class Producer:
        def __init__(self, conf):
            self.inited = False
            calls.append(("Producer", conf["transactional.id"]))
        def init_transactions(self):
            if self.inited:                      # как настоящий librdkafka: «Operation not valid in state Ready»
                raise KafkaException(Err(code=-172))
            self.inited = True
            calls.append(("init_transactions",))
        def begin_transaction(self): calls.append(("begin",))
        def produce(self, topic, key=None, value=None, **kw): calls.append(("produce", topic, key))
        def send_offsets_to_transaction(self, offsets, meta):
            calls.append(("send_offsets", [(o.topic, o.partition, o.offset) for o in offsets], meta))
        def commit_transaction(self):
            if state["fail"]:
                raise state["fail"]
            calls.append(("commit",))
        def abort_transaction(self): calls.append(("abort",))
        def flush(self, t): pass
        def poll(self, t): pass

    for n, v in dict(KafkaError=KafkaError, KafkaException=KafkaException, TopicPartition=TopicPartition,
                     Consumer=Consumer, Producer=Producer).items():
        setattr(m, n, v)
    return m, calls, state, Msg, Err, KafkaException, TopicPartition


def tele(unit, ts, lat=49.1288, lon=142.6601, speed=36.0):
    return dumps(telemetry_to_dict(Telemetry(unit, ts, lat, lon, speed)))


class KafkaGlue(unittest.TestCase):
    def setUp(self):
        (self.stub, self.calls, self.state, self.Msg, self.Err, self.KafkaException,
         self.TP) = make_stub()
        patcher = mock.patch.dict(sys.modules, {"confluent_kafka": self.stub})
        patcher.start()
        self.addCleanup(patcher.stop)
        sys.modules.pop("vqueue.kafka_io", None)
        self.kio = importlib.import_module("vqueue.kafka_io")
        self.addCleanup(lambda: sys.modules.pop("vqueue.kafka_io", None))
        self.transport = self.kio.KafkaTransport(self.kio.KafkaSettings())
        self.runner = Runner(CFG, self.transport, max_batch=100)

    def names(self):
        return [c[0] for c in self.calls]

    def assign(self, offset=0):
        self.runner.start()
        self.transport._on_assign(self.transport.consumer, [self.TP("telemetry.v1", 0, offset)])

    def test_on_assign_fences_before_reading_snapshot_and_assigns(self):
        self.assign()
        n = self.names()
        self.assertIn("subscribe", n)
        self.assertLess(n.index("init_transactions"), n.index("close"))     # читатель снимка закрывается после
        self.assertLess(n.index("init_transactions"), n.index("assign"))
        self.assertEqual(self.calls[-1][0], "assign")

    def test_rebalance_reassignment_gets_fresh_producer(self):
        """Ребаланс (например, вошёл второй экземпляр) заново выдаёт партиции: init_transactions нельзя
        вызывать повторно на том же продюсере — нужен новый (он же вытесняет зомби)."""
        self.assign()
        first = self.transport.producer
        self.transport._on_assign(self.transport.consumer, [self.TP("telemetry.v1", 0, 0)])
        self.assertIsNot(self.transport.producer, first)
        self.assertEqual(self.names().count("init_transactions"), 2)
        self.state["inbox"] = [self.Msg("telemetry.v1", 0, 0, b"T-01", tele("T-01", 1757930400))]
        self.assertEqual(self.runner.step(0), 1)                 # и после ребаланса коммит работает
        self.assertEqual(self.names()[-1], "commit")

    def test_standby_with_empty_assignment_does_not_fence_active_owner(self):
        """Резерв получает пустой список партиций: init_transactions() и чтение снимка ему не нужны,
        иначе он вытеснит активный экземпляр (тот же transactional.id)."""
        self.runner.start()
        self.transport._on_assign(self.transport.consumer, [])
        n = self.names()
        self.assertNotIn("init_transactions", n)
        self.assertNotIn("Producer", n)
        self.assertEqual(self.calls[-1], ("assign", []))
        self.assertIsNone(self.transport.producer)

    def test_transaction_wraps_outputs_snapshot_and_offsets(self):
        self.assign()
        self.state["inbox"] = [self.Msg("telemetry.v1", 0, 10 + i, b"T-01", tele("T-01", 1757930400 + i,
                                49.1288 + 0.0001 * i)) for i in range(3)]
        self.assertEqual(self.runner.step(0), 3)
        n = self.names()
        i0 = n.index("begin")
        self.assertEqual(n[i0], "begin")
        self.assertIn(("produce", "vqueue.state.v1", b"state"), self.calls)
        self.assertEqual(n[-1], "commit")
        self.assertLess(n.index("send_offsets"), n.index("commit"))
        self.assertEqual(self.calls[n.index("send_offsets")][1:], ([("telemetry.v1", 0, 13)], "GROUP_META"))
        outs = {c[1] for c in self.calls if c[0] == "produce"}
        self.assertTrue(outs <= {"queue.v1", "decision.v1", "vqueue.state.v1"})

    def test_failed_commit_aborts_and_seeks_back(self):
        self.assign()
        self.state["inbox"] = [self.Msg("telemetry.v1", 0, 20, b"T-01", tele("T-01", 1757930400))]
        self.state["fail"] = self.KafkaException(self.Err(code=1))
        self.assertEqual(self.runner.step(0), 0)
        self.assertIn("abort", self.names())
        self.assertIn(("seek", 0, 20), self.calls)
        self.assertEqual(self.runner.stats["failed_commits"], 1)
        self.assertEqual(self.runner.engine.now, None)           # память откатилась

    def test_fenced_is_fatal(self):
        self.assign()
        self.state["inbox"] = [self.Msg("telemetry.v1", 0, 0, b"T-01", tele("T-01", 1757930400))]
        self.state["fail"] = self.KafkaException(self.Err(code=-144))
        with self.assertRaises(FatalTransportError):
            self.runner.step(0)
        self.assertNotIn("abort", self.names())

    def test_restores_state_and_offset_from_snapshot(self):
        # первый «процесс»: обработал пакет и записал снимок
        self.assign()
        self.state["inbox"] = [self.Msg("telemetry.v1", 0, 5 + i, b"T-01", tele("T-01", 1757930400 + i,
                                49.1288 + 0.0001 * i)) for i in range(4)]
        self.runner.step(0)
        snap = next(c for c in self.calls if c[0] == "produce" and c[1] == "vqueue.state.v1")
        self.assertTrue(snap)
        # снимок, который записал бы транспорт, берём из раннера
        doc = dumps({"v": 1, "offsets": {"0": 9}, "engine": self.runner.engine.snapshot()})
        self.state["snapshot"] = doc
        # «рестарт»: новый раннер и транспорт, брокер отдаёт снимок
        self.calls.clear()
        t2 = self.kio.KafkaTransport(self.kio.KafkaSettings())
        r2 = Runner(CFG, t2)
        r2.start()
        parts = [self.TP("telemetry.v1", 0, -1001)]
        t2._on_assign(t2.consumer, parts)
        self.assertEqual(r2.engine.now, self.runner.engine.now)
        self.assertEqual(parts[0].offset, 9)                       # встали на записанный оффсет
        self.assertEqual(self.calls[-1][0], "assign")

    def test_corrupted_snapshot_replays_from_beginning(self):
        self.state["snapshot"] = b"{broken"
        self.runner.start()
        parts = [self.TP("telemetry.v1", 0, -1001)]
        self.transport._on_assign(self.transport.consumer, parts)
        self.assertEqual(parts[0].offset, -2)                      # OFFSET_BEGINNING


if __name__ == "__main__":
    unittest.main()
