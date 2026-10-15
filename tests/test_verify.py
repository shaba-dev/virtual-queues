"""Верификатор, режим replay и паритет «генератор -> движок» с офлайн-симуляцией."""
import contextlib
import copy
import io
import json
import os
import tempfile
import unittest
import unittest.mock

from vqueue import generator
from vqueue.codec import decision_to_dict, queue_to_dict
from vqueue.config import SiteConfig
from vqueue.engine import DecisionEvent, QueueEvent
from vqueue.simulate import replay, run
from vqueue import verify as verify_module
from vqueue.verify import check_streams, compare, main as verify_main

CFG = SiteConfig.load("config/site.json")
T0 = 1757930400
ARGS = ["--stdout", "--duration", "900", "--speedup", "0", "--start-ts", str(T0), "--seed", "1",
        "--dup-rate", "0.02", "--late-rate", "0.02"]


def docs(events):
    q = [queue_to_dict(e) for e in events if isinstance(e, QueueEvent)]
    d = [decision_to_dict(e) for e in events if isinstance(e, DecisionEvent)]
    return q, d


class VerifyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):                 # настоящий генератор в режиме --stdout
            generator.main(ARGS)
        cls.lines = buf.getvalue().splitlines()
        cls.eng, cls.events, cls.bad = replay(CFG, cls.lines)
        cls.q, cls.d = docs(cls.events)

    def test_generator_output_is_valid_telemetry(self):
        self.assertEqual(self.bad, 0)
        self.assertGreater(len(self.lines), 900 * 40)         # плюс дубли

    def test_replay_equals_offline_simulation(self):
        """Поток генератора через движок == прогон симулятора: эталон можно получать любым путём."""
        _, _, ev = run(CFG, T0, 900, seed=1, dup_rate=0.02, late_rate=0.02, follow_decisions=False)
        self.assertEqual(self.events, ev)

    def test_real_output_passes_checks(self):
        self.assertEqual(check_streams(self.q, self.d, CFG), [])
        self.assertGreater(len(self.q), 100)
        self.assertGreater(len(self.d), 10)

    def test_corrupted_queue_is_detected(self):
        bad = copy.deepcopy(self.q)
        i = next(i for i, x in enumerate(bad) if len(x["queue"]) >= 2)
        bad[i]["queue"][1]["wait_seconds"] += 7
        self.assertTrue(any("wait_seconds" in p for p in check_streams(bad, self.d, CFG)))

    def test_duplicate_decision_is_detected(self):
        bad = self.d + [self.d[0]]
        self.assertTrue(any("дубль" in p for p in check_streams(self.q, bad, CFG)))

    def test_repeated_queue_is_detected(self):
        bad = [self.q[0], dict(self.q[0])] + self.q[1:]            # то же содержимое подряд у одной станции
        self.assertTrue(any("повтор" in p for p in check_streams(bad, self.d, CFG)))

    def test_bad_gain_is_detected(self):
        rec = next(x for x in self.d if x["result"] == "recommended")
        bad = [dict(rec, gain_seconds=10)]
        self.assertTrue(any("порога" in p for p in check_streams([], bad, CFG)))

    def test_compare(self):
        self.assertEqual(compare(self.d, list(self.d), "unit_uuid"), [])
        self.assertEqual(compare(self.q, list(reversed(self.q))[::-1], "station_uuid"), [])
        self.assertTrue(compare(self.d, self.d[:-1], "unit_uuid"))
        mutated = copy.deepcopy(self.d)
        mutated[0]["at"] = "2000-01-01T00:00:00Z"
        self.assertTrue(compare(self.d, mutated, "unit_uuid"))
        self.assertTrue(compare(self.d, self.d + [dict(self.d[0], unit_uuid="T-99")], "unit_uuid"))

    def test_compare_ignores_order_between_different_keys(self):
        shuffled = sorted(self.d, key=lambda x: (x["unit_uuid"], x["at"]))        # другой общий порядок
        self.assertEqual(compare(self.d, shuffled, "unit_uuid"), [])

    def test_generator_out_file_equals_stdout_and_is_utf8_without_bom(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "t.jsonl")
            generator.main([a for a in ARGS if a != "--stdout"] + ["--out", path])
            raw = open(path, "rb").read()
            self.assertFalse(raw.startswith(b"\xef\xbb\xbf") or raw.startswith(b"\xff\xfe"))
            self.assertNotIn(b"\r", raw)
            self.assertEqual(raw.decode("utf-8").splitlines(), self.lines)

    def test_cli_compare_dedup_actual(self):
        with tempfile.TemporaryDirectory() as tmp:
            g, a = os.path.join(tmp, "g.jsonl"), os.path.join(tmp, "a.jsonl")
            for path, docs in ((g, self.d), (a, self.d + self.d)):
                with open(path, "w", encoding="utf-8") as f:
                    f.writelines(json.dumps(x, ensure_ascii=False) + "\n" for x in docs)
            args = ["compare", "--golden", g, "--actual", a, "--key", "unit_uuid"]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(verify_main(args), 1)                       # дубли без --dedup-actual — провал
                self.assertEqual(verify_main(args + ["--dedup-actual"]), 0)  # после удаления повторов — совпадение

    def test_full_mode_reports_empty_output_instead_of_confusing_compare(self):
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(buf), \
                unittest.mock.patch.object(verify_module, "consume_topics",
                                           return_value={"queue.v1": [], "decision.v1": []}):
            code = verify_main(["full", "--queue-out", os.path.join(tmp, "q"), "--decision-out", os.path.join(tmp, "d"),
                                "--golden-queue", "nonexistent-not-read.jsonl"])
        self.assertEqual(code, 1)
        out = buf.getvalue()
        self.assertIn("выходные топики пусты", out)
        self.assertIn("сервис не запущен", out)
        self.assertNotIn("OK    формат", out)                    # не изображаем успех на пустом выходе


if __name__ == "__main__":
    unittest.main()
