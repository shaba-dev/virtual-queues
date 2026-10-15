"""Рекомендация: выбор станции, пороги, отказы, один расчёт за заезд."""
import unittest

from vqueue.engine import Engine
from tests.helpers import BASE, decisions, feed, m, make_cfg, rows

UNITS = {"T1": "S1", "T2": "S1", "T3": "S1", "T4": "S1", "T9": "S2", "T10": "S2"}


def example_events():
    e = Engine(make_cfg(units=UNITS))
    ev = feed(e, m("T1", BASE - 60, 15), m("T1", BASE, 15),
              m("T2", BASE, 1200, 36), m("T3", BASE, 3000, 36), m("T4", BASE, 900, 36))
    return e, ev


class Choice(unittest.TestCase):
    def test_recommends_free_station_with_gain_80(self):
        _, ev = example_events()
        d = decisions(ev)
        self.assertEqual([(x.unit_uuid, x.result) for x in d],
                         [("T2", "rejected"), ("T4", "recommended")])
        t4 = d[1]
        self.assertEqual((t4.at, t4.from_station, t4.to_station, t4.gain_seconds),
                         (BASE, "S1", "S2", 80))
        self.assertEqual(d[0].reason, "no_gain")            # у T2 выигрыш 50 с < 60 с

    def test_gain_threshold_is_inclusive_60_s(self):
        def scenario(s2_north):
            # T4 в 900 м от S1; S1: T1 занимает станцию в BASE-10; S2: T9 — в BASE-20
            e = Engine(make_cfg({"S1": 0.0, "S2": s2_north}, UNITS))
            return decisions(feed(e, m("T1", BASE - 10, 5), m("T9", BASE - 20, s2_north + 5),
                                  m("T4", BASE, 900, 36)))[0]
        ok = scenario(2300)        # до S2 1400 м: ожидание 70 с против 130 -> выигрыш 60
        self.assertEqual((ok.result, ok.gain_seconds), ("recommended", 60))
        no = scenario(2290)        # до S2 1390 м: выигрыш 59
        self.assertEqual((no.result, no.reason), ("rejected", "no_gain"))

    def test_waits_count_only_units_arriving_earlier(self):
        e = Engine(make_cfg(units=UNITS))
        # T2 приедет позже T4 (1200 м против 900 м) и в ожидание T4 не входит
        feed(e, m("T1", BASE - 60, 15), m("T2", BASE, 1200, 36))   # T2 остаётся на S1 (выигрыш 50 с)
        self.assertEqual(e.units["T2"].target, "S1")
        d = decisions(feed(e, m("T4", BASE, 900, 36)))[0]
        self.assertEqual(d.gain_seconds, 80)                # ожидание = T1.free_at (BASE+170) - (BASE+90); T2 не в счёт

    def test_tie_prefers_own_station(self):
        e = Engine(make_cfg(units=UNITS))                   # все станции свободны
        d = decisions(feed(e, m("T4", BASE, 900, 36)))[0]
        self.assertEqual((d.result, d.reason), ("rejected", "no_gain"))
        self.assertEqual(e.units["T4"].target, "S1")

    def test_tie_between_other_stations_prefers_nearest(self):
        e = Engine(make_cfg({"S1": 0.0, "S2": 5000.0, "S3": 2000.0}, UNITS))
        feed(e, m("T1", BASE - 10, 5))                      # у S1 очередь, у S2 и S3 пусто
        d = decisions(feed(e, m("T4", BASE, 900, 36)))[0]
        self.assertEqual((d.result, d.to_station), ("recommended", "S3"))   # S3 ближе (1100 м против 4100 м)

    def test_chooses_minimum_wait_not_nearest(self):
        e = Engine(make_cfg({"S1": 0.0, "S2": 1500.0, "S3": 9000.0}, {**UNITS, "T11": "S2", "T12": "S2"}))
        feed(e, m("T1", BASE - 10, 5),                      # S1 занята
             m("T9", BASE - 10, 1505), m("T10", BASE - 5, 1500), m("T11", BASE - 1, 1500))  # у S2 очередь
        d = decisions(feed(e, m("T4", BASE, 900, 36)))[0]
        self.assertEqual((d.result, d.to_station), ("recommended", "S3"))


class Lifecycle(unittest.TestCase):
    def test_one_decision_per_trip(self):
        e = Engine(make_cfg(units=UNITS))
        ev = feed(e, m("T4", BASE, 1400, 36), m("T4", BASE + 1, 1390, 36), m("T4", BASE + 2, 1380, 36))
        self.assertEqual(len(decisions(ev)), 1)

    def test_no_decision_outside_1500_m(self):
        e = Engine(make_cfg(units=UNITS))
        self.assertEqual(decisions(feed(e, m("T4", BASE, 1500, 36))), [])    # ровно 1500 — ещё не «ближе»
        self.assertEqual(len(decisions(feed(e, m("T4", BASE + 1, 1490, 36)))), 1)

    def test_stale_telemetry_rejection(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T3", BASE + 100, 9000, 36))                # «сейчас» = BASE+100
        d = decisions(feed(e, m("T4", BASE + 69, 1000, 36)))[0]     # на 31 с старше «сейчас»
        self.assertEqual((d.result, d.reason, d.at), ("rejected", "stale_telemetry", BASE + 100))

    def test_exactly_30_s_old_is_not_stale(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T3", BASE + 100, 9000, 36))
        d = decisions(feed(e, m("T4", BASE + 70, 1000, 36)))[0]
        self.assertEqual(d.reason, "no_gain")

    def test_stale_rejection_still_uses_up_the_trip(self):
        e = Engine(make_cfg(units=UNITS))
        feed(e, m("T3", BASE + 100, 9000, 36))
        feed(e, m("T4", BASE + 60, 1000, 36))
        self.assertEqual(decisions(feed(e, m("T4", BASE + 101, 800, 36))), [])

    def test_recommended_unit_is_queued_at_new_station_and_leaves_old(self):
        e, _ = example_events()
        self.assertNotIn("T4", [r[0] for r in rows(e.queue("S1"))])
        self.assertEqual([r[0] for r in rows(e.queue("S2"))], ["T4"])

    def test_after_service_unit_returns_to_own_station_and_gets_new_decision(self):
        e, _ = example_events()
        t = BASE
        feed(e, m("T4", t + 240, 3300, 0))                        # приехала к S2 и обслуживается
        self.assertEqual(e.units["T4"].station, "S2")
        self.assertFalse(e.units["T4"].decided)                   # побывала на станции
        feed(e, m("T4", t + 446, 3360, 36))                       # уехала от S2
        self.assertEqual(e.units["T4"].target, "S1")              # следующий заезд — к своей станции
        feed(e, m("T4", t + 1100, -2970, 36), m("T4", t + 1110, -2900, 36))   # разгрузка и обратно
        ev = feed(e, m("T4", t + 1500, 1400, 36))                 # снова ближе 1500 м к S1
        self.assertEqual(len(decisions(ev)), 1)

    def test_no_new_decision_until_visited_station(self):
        e, _ = example_events()
        # едет к S2, снова оказывается ближе 1500 м к S1: решение не считается
        self.assertEqual(decisions(feed(e, m("T4", BASE + 5, 800, 36))), [])


if __name__ == "__main__":
    unittest.main()
