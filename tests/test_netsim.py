import random

import numpy as np

from ddr.netsim import PROFILES, NetConditions, NetworkSimulator


def test_delay_and_jitter_statistics():
    sim = NetworkSimulator(NetConditions(delay_ms=10, jitter_ms=2), random.Random(1))
    d = np.array([sim.leg_delay_s() * 1e3 for _ in range(20000)])
    assert abs(d.mean() - 10) < 0.1
    assert abs(d.std() - 2) < 0.1


def test_loss_adds_retransmission_timeout():
    sim = NetworkSimulator(NetConditions(delay_ms=1, loss=0.1, rto_ms=200), random.Random(2))
    d = np.array([sim.leg_delay_s() * 1e3 for _ in range(20000)])
    assert abs((d > 100).mean() - 0.1) < 0.01


def test_bandwidth_serialisation_delay():
    sim = NetworkSimulator(NetConditions(bandwidth_mbps=8))
    assert abs(sim.leg_delay_s(1_000_000) - 1.0) < 1e-9  # 1 MB at 8 Mbit/s = 1 s


def test_delay_never_negative():
    sim = NetworkSimulator(NetConditions(delay_ms=0.1, jitter_ms=5), random.Random(3))
    assert min(sim.leg_delay_s() for _ in range(5000)) >= 0


def test_profiles_and_update(monkeypatch):
    c = NetConditions()
    c.update({"profile": "wan", "loss": 0.5})
    assert c.delay_ms == PROFILES["wan"]["delay_ms"] and c.loss == 0.5
    monkeypatch.setenv("NET_PROFILE", "lan")
    monkeypatch.setenv("NET_JITTER_MS", "3")
    e = NetConditions.from_env()
    assert e.delay_ms == PROFILES["lan"]["delay_ms"] and e.jitter_ms == 3


def test_fail_rate():
    sim = NetworkSimulator(NetConditions(fail_rate=0.25), random.Random(4))
    assert abs(np.mean([sim.should_fail() for _ in range(20000)]) - 0.25) < 0.02
