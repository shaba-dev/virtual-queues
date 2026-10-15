"""Расчёт очереди: пример из задания, порядок, горизонт, свежесть."""
import unittest

from vqueue.engine import Engine
from tests.helpers import BASE, feed, m, make_cfg, rows

UNITS = {"T1": "S1", "T2": "S1", "T3": "S1", "T4": "S1"}


class SpecExample(unittest.TestCase):
    def test_queue_example_from_task(self):
        e = Engine(make_cfg(units=UNITS))
        # T1 стоит в 15 м от S1, первое такое сообщение в 11:59:00; расчёт в 12:00:00
        feed(e, m("T1", BASE - 60, 15), m("T1", BASE, 15),
             m("T2", BASE, 1200, 36), m("T3", BASE, 3000, 36))
        self.assertEqual(rows(e.queue("S1")), [
            ("T1", None, BASE - 60, BASE + 170, 0),          # 11:59:00 -> 12:02:50
            ("T2", BASE + 120, BASE + 170, BASE + 400, 50),  # 12:02:00, старт 12:02:50, 12:06:40
            ("T3", BASE + 300, BASE + 400, BASE + 630, 100)  # 12:05:00, старт 12:06:40, 12:10:30
        ])

    def test_decision_example_from_task_changes_queues(self):
        e = Engine(make_cfg(units=UNITS))                   # S2 в 2400 м от T4 (T4 в 900 м от S1)
        feed(e, m("T1", BASE - 60, 15), m("T1", BASE, 15),
             m("T2", BASE, 1200, 36), m("T3", BASE, 3000, 36))
        s1_before = rows(e.queue("S1"))
        feed(e, m("T4", BASE, 900, 36))
        self.assertEqual(rows(e.queue("S1")), s1_before)    # T4 из очереди S1 исключена
        self.assertEqual(rows(e.queue("S2")), [("T4", BASE + 240, BASE + 240, BASE + 470, 0)])


class QueueRules(unittest.TestCase):
    def test_no_wait_when_arriving_after_station_is_free(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T1", BASE, 15), m("T2", BASE, 4000, 36))
        self.assertEqual(rows(e.queue("S1"))[1], ("T2", BASE + 400, BASE + 400, BASE + 630, 0))

    def test_ordered_by_eta_not_by_message_order(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T3", BASE, 6000, 36), m("T2", BASE, 5000, 36))
        self.assertEqual([r[0] for r in rows(e.queue("S1"))], ["T2", "T3"])

    def test_horizon_is_30_minutes_inclusive(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T1", BASE, 18000, 36), m("T2", BASE, 18010, 36))   # приезд 1800 с и 1801 с
        self.assertEqual([r[0] for r in rows(e.queue("S1"))], ["T1"])

    def test_stale_position_is_excluded_after_30_s(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T2", BASE, 3000, 36), m("T3", BASE + 30, 5000, 36))
        self.assertEqual([r[0] for r in rows(e.queue("S1"))], ["T2", "T3"])    # ровно 30 с — свежая
        feed(e, m("T3", BASE + 31, 4990, 36))
        self.assertEqual([r[0] for r in rows(e.queue("S1"))], ["T3"])          # 31 с — устарела

    def test_stale_unit_returns_with_fresh_position(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T2", BASE, 3000, 36), m("T3", BASE + 60, 5000, 36))
        self.assertEqual([r[0] for r in rows(e.queue("S1"))], ["T3"])
        feed(e, m("T2", BASE + 61, 2390, 36))
        self.assertEqual([r[0] for r in rows(e.queue("S1"))], ["T2", "T3"])

    def test_occupant_stays_in_queue_without_telemetry(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T1", BASE, 15), m("T2", BASE + 100, 3000, 36))      # T1 молчит 100 с
        self.assertEqual(rows(e.queue("S1"))[0], ("T1", None, BASE, BASE + 230, 0))

    def test_unit_going_elsewhere_is_not_in_foreign_queue(self):
        e = Engine(make_cfg(units={"T1": "S1", "T2": "S2"}))
        feed(e, m("T1", BASE, 5000, 36))
        self.assertEqual(e.queue("S2"), ())


if __name__ == "__main__":
    unittest.main()
