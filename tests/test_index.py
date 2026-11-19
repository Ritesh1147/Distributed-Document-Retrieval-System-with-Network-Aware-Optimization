import numpy as np
import pytest

from ddr.corpus import make_queries
from ddr.index import GlobalStats, ShardIndex, merge_topk


@pytest.fixture(scope="module")
def central(corpus):
    return ShardIndex(corpus, 0, 1)


@pytest.fixture(scope="module", params=[2, 3, 5])
def shards(request, corpus):
    return [ShardIndex(corpus, i, request.param) for i in range(request.param)]


def test_shards_partition_corpus(corpus, shards):
    ids = np.concatenate([s.doc_ids for s in shards])
    assert len(ids) == corpus.num_docs
    assert len(np.unique(ids)) == corpus.num_docs
    assert sum(s.num_postings for s in shards) >= 0


def test_postings_match_raw_counts(corpus, central):
    doc = 123
    toks = corpus.doc_tokens(doc)
    t = int(toks[0])
    s, e = central.term_offsets[t], central.term_offsets[t + 1]
    i = np.flatnonzero(central.doc_ids[central.post_docs[s:e]] == doc)
    assert central.post_tf[s:e][i[0]] == np.count_nonzero(toks == t)


def test_global_stats_reproduce_centralized_ranking(corpus, central, shards):
    """dfs_query_then_fetch must be exactly equivalent to a single index."""
    for q in make_queries(corpus, 150, seed=5):
        tids = central.parse(q)
        expected = central.search(tids, 10)
        stats = GlobalStats.from_json(GlobalStats.merge([s.term_stats(tids) for s in shards]).to_json())
        got = merge_topk([s.search(tids, 10, stats) for s in shards], 10)
        assert [r["doc_id"] for r in got] == [r["doc_id"] for r in expected]
        assert np.allclose([r["score"] for r in got], [r["score"] for r in expected])


def test_local_stats_are_close_but_approximate(corpus, central, shards):
    overlap = []
    for q in make_queries(corpus, 150, seed=6):
        tids = central.parse(q)
        a = {r["doc_id"] for r in central.search(tids, 10)}
        b = {r["doc_id"] for r in merge_topk([s.search(tids, 10) for s in shards], 10)}
        overlap.append(len(a & b) / max(len(a), 1))
    assert np.mean(overlap) > 0.8


def test_results_sorted_and_deterministic(central, corpus):
    q = make_queries(corpus, 1, seed=9)[0]
    r1 = central.search(central.parse(q), 20)
    r2 = central.search(central.parse(q), 20)
    assert r1 == r2
    keys = [(-r["score"], r["doc_id"]) for r in r1]
    assert keys == sorted(keys)


def test_unknown_terms_and_empty_query(central):
    assert central.search(central.parse("zzzz qqqq"), 10) == []
    assert central.search([], 10) == []


def test_k_larger_than_shard(corpus):
    tiny = ShardIndex(corpus, 0, corpus.num_docs // 3)
    q = corpus.vocab[int(corpus.doc_tokens(0)[0])]
    assert len(tiny.search(tiny.parse(q), 50)) <= tiny.num_docs


def test_merge_topk_tiebreak():
    a = [{"doc_id": 5, "score": 1.0}, {"doc_id": 1, "score": 0.5}]
    b = [{"doc_id": 2, "score": 1.0}]
    assert [r["doc_id"] for r in merge_topk([a, b], 2)] == [2, 5]


def test_invalid_shard(corpus):
    with pytest.raises(ValueError):
        ShardIndex(corpus, 3, 3)
