"""Vectorised BM25 inverted index over one shard of the corpus.

Documents are assigned to shards by ``doc_id % num_shards`` (hash partitioning
on the document id), so every shard holds a statistically similar slice of the
corpus and query load is spread evenly across shards.

Two scoring modes support the classic distributed-IR trade-off:

* **local statistics** (``query_then_fetch``): each shard scores with its own
  document frequencies / average length. One round trip, but scores are only
  approximately comparable across shards.
* **global statistics** (``dfs_query_then_fetch``): the router first collects
  per-term document frequencies from all shards, then passes the aggregated
  statistics in the search call. Scores are then bit-identical to a
  centralized index, at the cost of an extra fan-out round trip.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

from .corpus import Corpus

K1 = 1.2
B = 0.75


@dataclass
class GlobalStats:
    num_docs: int
    avgdl: float
    df: dict[int, int]

    @classmethod
    def merge(cls, parts: list[dict]) -> "GlobalStats":
        n = sum(p["num_docs"] for p in parts)
        total_len = sum(p["total_len"] for p in parts)
        df: dict[int, int] = {}
        for p in parts:
            for t, c in p["df"].items():
                df[int(t)] = df.get(int(t), 0) + int(c)
        return cls(num_docs=n, avgdl=total_len / max(n, 1), df=df)

    def to_json(self) -> dict:
        return {"num_docs": self.num_docs, "avgdl": self.avgdl, "df": {str(k): v for k, v in self.df.items()}}

    @classmethod
    def from_json(cls, d: dict) -> "GlobalStats":
        return cls(num_docs=int(d["num_docs"]), avgdl=float(d["avgdl"]),
                   df={int(k): int(v) for k, v in d["df"].items()})


def idf(n: int, df: int) -> float:
    return math.log(1.0 + (n - df + 0.5) / (df + 0.5))


class ShardIndex:
    def __init__(self, corpus: Corpus, shard_id: int = 0, num_shards: int = 1):
        if not 0 <= shard_id < num_shards:
            raise ValueError(f"shard_id {shard_id} out of range for {num_shards} shards")
        self.shard_id = shard_id
        self.num_shards = num_shards
        self.vocab = corpus.vocab
        self.term_ids = {w: i for i, w in enumerate(corpus.vocab)}
        self.doc_ids = np.arange(shard_id, corpus.num_docs, num_shards, dtype=np.int64)  # local -> global
        n_local = len(self.doc_ids)

        starts = corpus.offsets[self.doc_ids]
        ends = corpus.offsets[self.doc_ids + 1]
        self.doc_len = (ends - starts).astype(np.float64)
        self.total_len = float(self.doc_len.sum())
        self.avgdl = self.total_len / max(n_local, 1)

        # Gather this shard's tokens and build (term, local_doc) -> tf postings.
        idx = np.concatenate([np.arange(s, e) for s, e in zip(starts, ends)]) if n_local else np.zeros(0, np.int64)
        local_doc = np.repeat(np.arange(n_local, dtype=np.int64), (ends - starts))
        terms = corpus.tokens[idx].astype(np.int64)
        keys, tf = np.unique(terms * max(n_local, 1) + local_doc, return_counts=True)
        post_terms = keys // max(n_local, 1)
        self.post_docs = (keys % max(n_local, 1)).astype(np.int32)
        self.post_tf = tf.astype(np.float64)
        self.term_offsets = np.searchsorted(post_terms, np.arange(len(self.vocab) + 1)).astype(np.int64)
        self.df = np.diff(self.term_offsets)

        # Precompute a short snippet per doc (term ids, -1 padded) so serving never
        # page-faults into the memory-mapped corpus.
        n_snip = 10
        pos = starts[:, None] + np.arange(n_snip)[None, :]
        valid = pos < ends[:, None]
        self._snippets = np.where(valid, np.asarray(corpus.tokens[np.minimum(pos, len(corpus.tokens) - 1)]), -1
                                  ).astype(np.int32)

    # -- introspection ---------------------------------------------------
    @property
    def num_docs(self) -> int:
        return len(self.doc_ids)

    @property
    def num_postings(self) -> int:
        return len(self.post_docs)

    def parse(self, query: str) -> list[int]:
        seen, out = set(), []
        for w in query.lower().split():
            t = self.term_ids.get(w)
            if t is not None and t not in seen:
                seen.add(t)
                out.append(t)
        return out

    def term_stats(self, term_ids: list[int]) -> dict:
        return {
            "num_docs": self.num_docs,
            "total_len": self.total_len,
            "df": {str(t): int(self.df[t]) for t in term_ids if 0 <= t < len(self.df)},
        }

    def snippet(self, local_doc: int) -> str:
        return " ".join(self.vocab[t] for t in self._snippets[local_doc] if t >= 0)

    # -- search ------------------------------------------------------------
    def search(self, term_ids: list[int], k: int = 10, stats: GlobalStats | None = None) -> list[dict]:
        if not term_ids or self.num_docs == 0:
            return []
        n = stats.num_docs if stats else self.num_docs
        avgdl = stats.avgdl if stats else self.avgdl
        scores = np.zeros(self.num_docs, dtype=np.float64)
        norm = K1 * (1.0 - B + B * self.doc_len / avgdl)
        for t in term_ids:
            s, e = self.term_offsets[t], self.term_offsets[t + 1]
            if s == e:
                continue
            df = stats.df.get(t, int(self.df[t])) if stats else int(self.df[t])
            docs = self.post_docs[s:e]
            tf = self.post_tf[s:e]
            scores[docs] += idf(n, df) * tf * (K1 + 1.0) / (tf + norm[docs])

        k = min(k, self.num_docs)
        if k < self.num_docs:
            # Include every doc tied with the k-th score so the cut below is
            # deterministic (argpartition breaks ties arbitrarily).
            kth = scores[np.argpartition(-scores, k - 1)[k - 1]]
            cand = np.flatnonzero(scores >= kth)
        else:
            cand = np.arange(self.num_docs)
        cand = cand[scores[cand] > 0]
        # Deterministic ordering: score desc, then global doc id asc.
        order = np.lexsort((self.doc_ids[cand], -scores[cand]))[:k]
        return [
            {"doc_id": int(self.doc_ids[c]), "score": float(scores[c]), "shard": self.shard_id,
             "snippet": self.snippet(int(c))}
            for c in cand[order]
        ]


def merge_topk(result_lists: list[list[dict]], k: int) -> list[dict]:
    """Merge per-shard top-k lists into a global top-k (score desc, doc_id asc)."""
    allr = [r for lst in result_lists for r in lst]
    allr.sort(key=lambda r: (-r["score"], r["doc_id"]))
    return allr[:k]
