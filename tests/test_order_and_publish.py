"""Порядок и дубли, «сейчас», публикация очередей, кодек."""
import json
import unittest

from vqueue.codec import decision_to_dict, iso, parse_telemetry, queue_to_dict
from vqueue.engine import DecisionEvent, Engine, QueueEntry, QueueEvent
from tests.helpers import BASE, feed, m, make_cfg, queues, rows

UNITS = {"T1": "S1", "T2": "S1", "T3": "S1"}


class OrderAndDuplicates(unittest.TestCase):
    def test_duplicate_is_ignored(self):
        e = Engine(make_cfg(units=UNITS))
        first = feed(e, m("T2", BASE, 3000, 36))
        again = feed(e, m("T2", BASE, 3000, 36))
        self.assertTrue(first)
        self.assertEqual(again, [])
        self.assertEqual(e.stats["ignored_old"], 1)

    def test_same_ts_different_content_is_ignored(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T2", BASE, 3000, 36), m("T2", BASE, 100, 36))
        self.assertEqual(rows(e.queue("S1"))[0][1], BASE + 300)

    def test_out_of_order_older_message_is_ignored(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T2", BASE + 10, 3000, 36), m("T2", BASE + 5, 100, 36))
        self.assertEqual(e.units["T2"].last_ts, BASE + 10)
        self.assertEqual(e.stats["ignored_old"], 1)

    def test_late_message_with_newer_ts_is_accepted_and_now_is_max(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T3", BASE + 100, 9000, 36))
        feed(e, m("T2", BASE + 60, 3000, 36))                  # опоздала на 40 с, но у T2 это первое сообщение
        self.assertEqual(e.now, BASE + 100)
        self.assertEqual(e.units["T2"].last_ts, BASE + 60)
        feed(e, m("T2", BASE + 70, 2900, 36))                  # новее предыдущего ts T2 — учитывается
        self.assertEqual(e.units["T2"].last_ts, BASE + 70)
        self.assertEqual(e.now, BASE + 100)

    def test_now_never_goes_back(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T2", BASE + 50, 9000, 36), m("T3", BASE + 10, 9000, 36))
        self.assertEqual(e.now, BASE + 50)

    def test_unknown_unit_is_ignored(self):
        e = Engine(make_cfg(units=UNITS))
        self.assertEqual(feed(e, m("X", BASE, 10)), [])
        self.assertEqual(e.stats["unknown_unit"], 1)

    def test_results_do_not_depend_on_wall_clock(self):
        a, b = Engine(make_cfg(units=UNITS)), Engine(make_cfg(units=UNITS))
        msgs = [m("T1", BASE, 15), m("T2", BASE, 1200, 36), m("T3", BASE, 3000, 36)]
        self.assertEqual(feed(a, *msgs), feed(b, *msgs))


class Publication(unittest.TestCase):
    def test_identical_consecutive_queues_are_not_republished(self):
        e = Engine(make_cfg(units=UNITS))
        ev = feed(e, m("T1", BASE, 15), m("T1", BASE + 1, 15), m("T1", BASE + 2, 15))
        self.assertEqual(len(queues(ev, "S1")), 1)

    def test_any_change_is_published(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T1", BASE, 15))
        self.assertEqual(len(queues(feed(e, m("T2", BASE + 1, 3000, 36)), "S1")), 1)   # новый состав
        # T2 едет не со скоростью 10 м/с: приезд сместился на 3 с -> изменилось время
        self.assertEqual(len(queues(feed(e, m("T2", BASE + 2, 2970, 36)), "S1")), 1)

    def test_moving_at_design_speed_does_not_change_eta(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T2", BASE, 3000, 36))
        self.assertEqual(queues(feed(e, m("T2", BASE + 1, 2990, 36), m("T2", BASE + 2, 2980, 36)), "S1"), [])

    def test_empty_queue_published_once_after_drain(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T1", BASE, 15))
        ev = feed(e, m("T1", BASE + 231, 15))
        (q,) = queues(ev, "S1")
        self.assertEqual(q.queue, ())
        self.assertEqual(queues(feed(e, m("T1", BASE + 232, 15)), "S1"), [])

    def test_empty_initial_queue_is_not_published(self):
        e = Engine(make_cfg(units=UNITS))
        self.assertEqual(queues(feed(e, m("T1", BASE, 10000, 36)), "S2"), [])


class Codec(unittest.TestCase):
    def test_queue_json_matches_task_format(self):
        ev = QueueEvent("S1", BASE, (
            QueueEntry("T1", None, BASE - 60, BASE + 170, 0),
            QueueEntry("T2", BASE + 120, BASE + 170, BASE + 400, 50)))
        self.assertEqual(queue_to_dict(ev), {"station_uuid": "S1", "at": "2026-09-15T12:00:00Z", "queue": [
            {"unit_uuid": "T1", "eta": None, "service_start": "2026-09-15T11:59:00Z",
             "free_at": "2026-09-15T12:02:50Z", "wait_seconds": 0},
            {"unit_uuid": "T2", "eta": "2026-09-15T12:02:00Z", "service_start": "2026-09-15T12:02:50Z",
             "free_at": "2026-09-15T12:06:40Z", "wait_seconds": 50}]})

    def test_decision_json_matches_task_format(self):
        rec = DecisionEvent("T4", BASE, "recommended", "S1", "S2", 80)
        rej = DecisionEvent("T4", BASE, "rejected", reason="no_gain")
        self.assertEqual(decision_to_dict(rec), {"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z",
                         "result": "recommended", "from_station": "S1", "to_station": "S2", "gain_seconds": 80})
        self.assertEqual(decision_to_dict(rej), {"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z",
                         "result": "rejected", "reason": "no_gain"})

    def test_parse_telemetry(self):
        raw = json.dumps({"unit_uuid": "T-12", "ts": 1757930400, "lat": 49.1288, "lon": 142.6601, "speed_kmh": 21.4})
        t = parse_telemetry(raw.encode())
        self.assertEqual((t.unit_uuid, t.ts, t.speed_kmh), ("T-12", 1757930400, 21.4))
        for bad in (b"not json", b"{}", b'{"unit_uuid":"a","ts":"x","lat":1,"lon":1,"speed_kmh":1}',
                    b'{"unit_uuid":"a","ts":1,"lat":999,"lon":1,"speed_kmh":1}',
                    b'{"unit_uuid":"a","ts":1,"lat":1,"lon":1,"speed_kmh":-5}'):
            self.assertIsNone(parse_telemetry(bad))

    def test_iso(self):
        self.assertEqual(iso(BASE), "2026-09-15T12:00:00Z")
        self.assertIsNone(iso(None))


if __name__ == "__main__":
    unittest.main()
