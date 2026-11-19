import importlib.util
import os

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("gen_compose", os.path.join(ROOT, "docker", "gen_compose.py"))
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)


def render(**kw):
    args = dict(shards=3, replicas=2, profile="lan", far_zone_profile=None, strategy="p2c", hedge="-1",
                cache=30, centralized=True)
    args.update(kw)
    return yaml.safe_load(gen.generate(**args))


def test_topology_services():
    d = render()
    svcs = d["services"]
    nodes = [s for s in svcs if s.startswith("dist-")]
    assert len(nodes) == 6
    assert {"redis", "router-distributed", "central-node", "router-centralized"} <= set(svcs)
    env = svcs["dist-s2r1"]["environment"]
    assert env["SHARD_ID"] == "2" and env["NUM_SHARDS"] == "3" and env["ZONE"] == "zone1"
    assert env["REDIS_URL"].startswith("redis://redis")
    assert set(svcs["router-distributed"]["depends_on"]) == set(nodes)
    assert svcs["router-distributed"]["environment"]["LB_STRATEGY"] == "p2c"
    assert svcs["central-node"]["environment"]["NUM_SHARDS"] == "1"


def test_far_zone_profile_applies_to_last_replica_only():
    svcs = render(far_zone_profile="cross_zone")["services"]
    assert svcs["dist-s0r1"]["environment"]["NET_PROFILE"] == "cross_zone"
    assert svcs["dist-s0r0"]["environment"]["NET_PROFILE"] == "lan"


def test_without_centralized():
    assert "central-node" not in render(centralized=False)["services"]
