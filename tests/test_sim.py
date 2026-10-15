"""Симулятор и интеграция «симулятор -> движок» (без брокера)."""
import json
import unittest
from collections import defaultdict

from vqueue.codec import telemetry_to_dict
from vqueue.config import SiteConfig
from vqueue.engine import DecisionEvent, Engine, QueueEvent
from vqueue.geo import haversine_m
from vqueue.sim import Noise, Simulator
from vqueue.simulate import run

CFG = SiteConfig.load("config/site.json")
T0 = 1757930400


class SimulatorTests(unittest.TestCase):
    def test_one_message_per_second_per_unit(self):
        sim = Simulator(CFG, T0, seed=3)
        for i in range(5):
            batch = sim.tick()
            self.assertEqual(len(batch), 40)
            self.assertEqual({x.ts for x in batch}, {T0 + i})
            self.assertEqual(len({x.unit_uuid for x in batch}), 40)

    def test_deterministic_for_seed(self):
        a, b = Simulator(CFG, T0, 5), Simulator(CFG, T0, 5)
        self.assertEqual([a.tick() for _ in range(50)], [b.tick() for _ in range(50)])

    def test_constant_speed_10_ms_while_moving(self):
        sim = Simulator(CFG, T0, 2)
        prev = {x.unit_uuid: x for x in sim.tick()}
        checked = 0
        for _ in range(300):
            for x in sim.tick():
                p = prev[x.unit_uuid]
                if x.speed_kmh == 36.0 and p.speed_kmh == 36.0:
                    d = haversine_m(p.lat, p.lon, x.lat, x.lon)
                    if d > 0.5:
                        self.assertAlmostEqual(d, 10.0, delta=0.5)
                        checked += 1
                prev[x.unit_uuid] = x
        self.assertGreater(checked, 1000)

    def test_station_serves_one_at_a_time_200_s_with_30_s_gap(self):
        sim = Simulator(CFG, T0, 4)
        starts = defaultdict(list)
        seen = {}
        for _ in range(4000):
            sim.tick()
            for u, v in sim.vehicles.items():
                if v.phase == "service" and seen.get(u) != v.service_end:
                    seen[u] = v.service_end
                    starts[v.station].append(sim.t)
        for sid, ts in starts.items():
            self.assertGreater(len(ts), 5, sid)
            gaps = [b - a for a, b in zip(ts, ts[1:])]
            self.assertGreaterEqual(min(gaps), 230, sid)

    def test_vehicles_stand_still_on_station_for_200_s(self):
        sim = Simulator(CFG, T0, 4)
        st = sim.cfg.stations["S4"]
        stand = defaultdict(int)
        for _ in range(3000):
            for x in sim.tick():
                if x.speed_kmh < 1 and haversine_m(x.lat, x.lon, st.lat, st.lon) < 50:
                    stand[x.unit_uuid] += 1
        self.assertTrue(stand)
        self.assertGreaterEqual(max(stand.values()), 200)

    def test_redirect_goes_to_other_station(self):
        sim = Simulator(CFG, T0, 1)
        u = next(u for u, v in sim.vehicles.items() if v.home == "S1")
        self.assertTrue(sim.redirect(u, "S3"))
        self.assertEqual(sim.vehicles[u].station, "S3")
        self.assertFalse(sim.redirect(u, "S3"))


class NoiseTests(unittest.TestCase):
    def test_duplicates_and_lateness_within_60_s(self):
        sim, noise = Simulator(CFG, T0, 1), Noise(dup_rate=0.1, late_rate=0.1, max_late=60, seed=9)
        out = []
        for _ in range(300):
            t = sim.t + 1
            for m in noise.push(t, sim.tick()):
                out.append((t, m))
        delivered_in_flow = list(out)
        out.extend((sim.t + 100, m) for m in noise.flush())
        keys = [json.dumps(telemetry_to_dict(m)) for _, m in out]
        self.assertGreater(len(keys) - len(set(keys)), 100)                   # дубли есть
        self.assertLessEqual(max(t - m.ts for t, m in delivered_in_flow), 65)  # опоздание ≤ 60 (дубль — ещё ≤ 5)
        self.assertGreater(max(t - m.ts for t, m in delivered_in_flow), 30)    # и опоздания действительно длинные
        last, reordered = {}, 0
        for _, m in out:
            if m.ts < last.get(m.unit_uuid, 0):
                reordered += 1
            last[m.unit_uuid] = max(last.get(m.unit_uuid, 0), m.ts)
        self.assertGreater(reordered, 50)                                      # нарушение порядка есть

    def test_no_noise_passes_through_in_order(self):
        sim, noise = Simulator(CFG, T0, 1), Noise()
        for _ in range(20):
            batch = sim.tick()
            self.assertEqual(noise.push(sim.t, batch), batch)


class EndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sim, cls.eng, cls.events = run(CFG, T0, 3600, seed=11, dup_rate=0.02, late_rate=0.02)

    def test_queue_events_are_consistent(self):
        last_at, last_q = 0, {}
        n = 0
        for ev in self.events:
            if not isinstance(ev, QueueEvent):
                continue
            n += 1
            self.assertGreaterEqual(ev.at, last_at)
            last_at = ev.at
            self.assertNotEqual(ev.queue, last_q.get(ev.station_uuid, ()))      # одинаковые подряд не публикуются
            last_q[ev.station_uuid] = ev.queue
            units = [e.unit_uuid for e in ev.queue]
            self.assertEqual(len(units), len(set(units)))
            prev_free = None
            for i, e in enumerate(ev.queue):
                self.assertEqual(e.free_at, e.service_start + 230)
                self.assertGreaterEqual(e.wait_seconds, 0)
                if e.eta is None:
                    self.assertEqual(i, 0)                                      # занявшая станцию — первая
                    self.assertEqual(e.wait_seconds, 0)
                else:
                    self.assertEqual(e.wait_seconds, e.service_start - e.eta)
                    self.assertLessEqual(e.eta, ev.at + 1800)
                if prev_free is not None:
                    self.assertGreaterEqual(e.service_start, prev_free)
                prev_free = e.free_at
        self.assertGreater(n, 200)

    def test_decisions_are_consistent(self):
        ds = [e for e in self.events if isinstance(e, DecisionEvent)]
        self.assertGreater(len(ds), 30)
        rec = [d for d in ds if d.result == "recommended"]
        self.assertTrue(rec)
        for d in rec:
            self.assertGreaterEqual(d.gain_seconds, 60)
            self.assertNotEqual(d.from_station, d.to_station)
            self.assertEqual(d.from_station, CFG.assignments[d.unit_uuid])
        self.assertTrue(all(d.reason in ("no_gain", "stale_telemetry") for d in ds if d.result == "rejected"))
        per_unit = defaultdict(int)
        for d in ds:
            per_unit[d.unit_uuid] += 1
        self.assertGreater(sum(1 for c in per_unit.values() if c >= 2), 5)   # заезды повторяются

    def test_engine_tracks_sim_service(self):
        """Машина, обслуживаемая в симуляторе, у движка либо занимает станцию, либо стоит в её очереди."""
        sim, eng = Simulator(CFG, T0, 21), Engine(CFG)
        agree = total = 0
        for i in range(2400):
            for m in sim.tick():
                eng.process(m)
            if i < 1000:
                continue
            for u, v in sim.vehicles.items():
                if v.phase == "service" and sim.t - (v.service_end - 200) > 20:
                    total += 1
                    q = eng.queue(v.station)
                    agree += bool(q) and q[0].unit_uuid == u
        self.assertGreater(total, 300)
        self.assertGreater(agree / total, 0.85)


class SnapshotDeterminism(unittest.TestCase):
    def test_restore_gives_identical_future(self):
        sim, noise = Simulator(CFG, T0, 8), Noise(0.03, 0.03, seed=3)
        stream = []
        for _ in range(1000):
            stream.extend(noise.push(sim.t + 1, sim.tick()))
        full = Engine(CFG)
        out_full = [ev for m in stream for ev in full.process(m)]
        half = len(stream) // 2
        a = Engine(CFG)
        out_a = [ev for m in stream[:half] for ev in a.process(m)]
        b = Engine.restore(CFG, json.loads(json.dumps(a.snapshot())))          # через JSON, как в Kafka
        out_b = [ev for m in stream[half:] for ev in b.process(m)]
        self.assertEqual(out_a + out_b, out_full)
        self.assertEqual(b.snapshot(), full.snapshot())


if __name__ == "__main__":
    unittest.main()
