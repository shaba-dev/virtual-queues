"""Раннер: транзакционный пакет, рестарт из снимка, откат неудачного коммита."""
import json
import logging
import unittest

from vqueue.codec import dumps, telemetry_to_dict
from vqueue.config import SiteConfig
from vqueue.runner import Runner
from vqueue.sim import Noise, Simulator
from tests.fakes import FakeBroker, FakeTransport

logging.disable(logging.CRITICAL)      # ожидаемые сбои в тестах не засоряют вывод
CFG = SiteConfig.load("config/site.json")
T0 = 1757930400


def telemetry_log(seconds=600, seed=5):
    sim, noise = Simulator(CFG, T0, seed), Noise(0.03, 0.03, seed=seed)
    log = []
    for _ in range(seconds):
        log.extend(dumps(telemetry_to_dict(m)) for m in noise.push(sim.t + 1, sim.tick()))
    return log


def drain(runner, limit=10_000):
    for _ in range(limit):
        if runner.step(0) == 0 and runner.transport.pos >= len(runner.transport.b.log):
            return


def make(broker, **kw):
    r = Runner(CFG, FakeTransport(broker), max_batch=kw.pop("max_batch", 300), **kw)
    r.start()
    return r


def reference_output(log):
    b = FakeBroker()
    b.log = list(log)
    drain(make(b, max_batch=10_000))
    return b.out


class RunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.log = telemetry_log()
        cls.ref = reference_output(cls.log)

    def test_outputs_go_to_topics_with_keys(self):
        self.assertTrue(self.ref)
        for o in self.ref:
            doc = json.loads(o.value)
            if o.topic == "queue.v1":
                self.assertEqual(o.key.decode(), doc["station_uuid"])
            else:
                self.assertEqual(o.topic, "decision.v1")
                self.assertEqual(o.key.decode(), doc["unit_uuid"])
        self.assertEqual({o.topic for o in self.ref}, {"queue.v1", "decision.v1"})

    def test_batch_size_does_not_change_result(self):
        b = FakeBroker()
        b.log = list(self.log)
        drain(make(b, max_batch=17))
        self.assertEqual(b.out, self.ref)

    def test_restart_from_snapshot_gives_same_output_without_duplicates(self):
        b = FakeBroker()
        b.log = list(self.log)
        r = make(b, max_batch=250)
        for _ in range(12):
            r.step(0)
        self.assertLess(b.committed, len(self.log))
        r2 = make(b, max_batch=250)                        # «рестарт»: новый процесс, состояние из снимка
        self.assertEqual(r2.engine.now, r.engine.now)
        drain(r2)
        self.assertEqual(b.out, self.ref)

    def test_crash_before_commit_loses_nothing_and_duplicates_nothing(self):
        b = FakeBroker()
        b.log = list(self.log)
        r = make(b, max_batch=250)
        for _ in range(5):
            r.step(0)
        r.transport.poll(250, 0)                           # прочитал пакет и «упал» до коммита
        r2 = make(b, max_batch=250)
        drain(r2)
        self.assertEqual(b.out, self.ref)

    def test_failed_commit_is_rolled_back_and_retried(self):
        b = FakeBroker()
        b.log = list(self.log)
        r = make(b, max_batch=250)
        r.step(0)
        b.fail_commits = 2
        self.assertEqual(r.step(0), 0)
        self.assertEqual(r.step(0), 0)
        self.assertEqual(r.stats["failed_commits"], 2)
        drain(r)
        self.assertEqual(b.out, self.ref)

    def test_corrupted_snapshot_replays_from_start(self):
        b = FakeBroker()
        b.log = list(self.log)
        r = make(b, max_batch=250)
        for _ in range(6):
            r.step(0)
        b.snapshot = b"{broken"
        r2 = make(b, max_batch=250)
        self.assertEqual(r2.transport.pos, 0)
        drain(r2)
        # состояние восстановлено повтором; выходные сообщения повторного проигрывания — те же байты
        self.assertEqual(b.out[len(b.out) - len(self.ref):], self.ref)

    def test_invalid_messages_are_skipped_and_counted(self):
        b = FakeBroker()
        b.log = [b"garbage", b'{"unit_uuid":"T-01"}'] + list(self.log[:100])
        r = make(b)
        drain(r)
        self.assertEqual(r.stats["invalid"], 2)
        self.assertEqual(b.committed, len(b.log))


if __name__ == "__main__":
    unittest.main()
