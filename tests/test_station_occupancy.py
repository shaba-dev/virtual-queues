"""Занятие и освобождение станции."""
import unittest

from vqueue.engine import AT_STATION, TO_DUMP, TO_STATION, Engine
from tests.helpers import BASE, feed, m, make_cfg, rows

UNITS = {"T1": "S1", "T2": "S1", "T3": "S1"}


def engine():
    return Engine(make_cfg(units=UNITS))


class Occupancy(unittest.TestCase):
    def test_first_stopped_message_occupies_free_station(self):
        e = engine()
        feed(e, m("T1", BASE, 15, 0))
        self.assertEqual(rows(e.queue("S1")), [("T1", None, BASE, BASE + 230, 0)])
        self.assertEqual(e.units["T1"].phase, AT_STATION)

    def test_moving_inside_radius_does_not_occupy_until_stops(self):
        e = engine()
        feed(e, m("T1", BASE, 30, 36))
        self.assertEqual(e.units["T1"].phase, TO_STATION)
        (row,) = rows(e.queue("S1"))
        self.assertEqual(row[:2], ("T1", BASE + 3))            # едет: приезд через 30 м / 10 м/с
        feed(e, m("T1", BASE + 3, 0, 0))                       # остановилась на станции
        self.assertEqual(rows(e.queue("S1")), [("T1", None, BASE + 3, BASE + 233, 0)])

    def test_stopped_far_from_station_is_not_on_station(self):
        e = engine()
        feed(e, m("T1", BASE, 500, 0))                         # стоит, но далеко: состояние по умолчанию
        self.assertEqual(e.units["T1"].phase, TO_STATION)

    def test_released_after_230_s_even_if_still_in_radius(self):
        e = engine()
        feed(e, m("T1", BASE, 15), m("T2", BASE + 10, 10))     # T2 приехала, станция занята
        feed(e, m("T2", BASE + 229, 10))
        self.assertEqual([r[0] for r in rows(e.queue("S1"))], ["T1", "T2"])
        self.assertEqual(rows(e.queue("S1"))[1], ("T2", BASE + 10, BASE + 230, BASE + 460, 220))
        feed(e, m("T2", BASE + 231, 10))                       # T1 держит станцию уже >230 с
        self.assertEqual(rows(e.queue("S1")), [("T2", None, BASE + 230, BASE + 460, 0)])

    def test_released_when_occupant_leaves_radius_before_230_s(self):
        e = engine()
        feed(e, m("T1", BASE, 15), m("T2", BASE + 20, 10))
        feed(e, m("T2", BASE + 204, 10))                       # T2 ждёт и шлёт позицию (свежая)
        feed(e, m("T1", BASE + 205, 60, 36))                   # T1 вышла из радиуса 50 м
        self.assertEqual(e.units["T1"].phase, TO_DUMP)
        q = rows(e.queue("S1"))
        self.assertEqual(q[0], ("T2", None, BASE + 205, BASE + 435, 0))   # T2 занимает в момент выхода T1

    def test_waiter_arrival_is_radius_entry(self):
        e = engine()
        feed(e, m("T1", BASE, 15))
        feed(e, m("T2", BASE + 30, 40, 36), m("T2", BASE + 34, 0, 0))   # вошла в радиус в BASE+30, встала в +34
        q = rows(e.queue("S1"))
        self.assertEqual(q[1], ("T2", BASE + 30, BASE + 230, BASE + 460, 200))

    def test_served_overstayer_does_not_take_station_again(self):
        e = engine()
        feed(e, m("T1", BASE, 15), m("T1", BASE + 300, 15))
        self.assertEqual(e.queue("S1"), ())

    def test_stale_waiter_cannot_take_station(self):
        e = engine()
        feed(e, m("T1", BASE, 15), m("T2", BASE + 10, 10))
        feed(e, m("T3", BASE + 300, 5000, 36))                 # время идёт, T2 молчит (>30 с)
        self.assertNotIn("T2", [r[0] for r in rows(e.queue("S1"))])

    def test_unit_stopping_at_other_station_is_on_that_station(self):
        e = engine()
        feed(e, m("T1", BASE, 3300, 0))                        # своя S1, но стоит у S2
        self.assertEqual(e.units["T1"].station, "S2")
        self.assertEqual(rows(e.queue("S2")), [("T1", None, BASE, BASE + 230, 0)])
        self.assertEqual(e.queue("S1"), ())


class UnitStates(unittest.TestCase):
    def test_cycle_station_dump_station(self):
        e = engine()                                           # точка разгрузки в 3000 м к югу от S1
        feed(e, m("T1", BASE, 10, 0))
        self.assertEqual(e.units["T1"].phase, AT_STATION)
        feed(e, m("T1", BASE + 200, 40, 36))                   # ещё в радиусе станции
        self.assertEqual(e.units["T1"].phase, AT_STATION)
        feed(e, m("T1", BASE + 206, 60, 36))
        self.assertEqual(e.units["T1"].phase, TO_DUMP)
        feed(e, m("T1", BASE + 600, -2970, 36))                # в радиусе разгрузки
        self.assertEqual(e.units["T1"].phase, TO_DUMP)
        feed(e, m("T1", BASE + 610, -2900, 36))                # вышла из радиуса разгрузки
        self.assertEqual(e.units["T1"].phase, TO_STATION)

    def test_default_state_is_to_station(self):
        e = engine()
        feed(e, m("T1", BASE, 2000, 36))
        self.assertEqual(e.units["T1"].phase, TO_STATION)

    def test_to_dump_eta_is_path_via_dump(self):
        e = engine()
        feed(e, m("T1", BASE, 10), m("T1", BASE + 206, 60, 36))
        (row,) = rows(e.queue("S1"))
        # до разгрузки 3060 м + от разгрузки до станции 3000 м, на 10 м/с
        self.assertEqual(row[:2], ("T1", BASE + 206 + 606))


if __name__ == "__main__":
    unittest.main()
