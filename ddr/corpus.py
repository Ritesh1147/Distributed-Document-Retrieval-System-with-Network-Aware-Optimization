"""Deterministic synthetic corpus + query workload generator.

Real enterprise corpora are not redistributable, so we synthesise one with the
statistical properties that matter for retrieval cost and quality:

* a Zipf-distributed background vocabulary (a few very common terms with long
  posting lists, a long tail of rare terms),
* topical structure (each document is about one topic and over-samples that
  topic's words), so queries have genuinely relevant documents,
* log-normal document lengths (BM25 length normalisation matters).

Everything is generated with numpy from a single seed, so every node in the
cluster -- and the centralized baseline -- sees byte-identical data. The corpus
is written once to a directory of ``.npy`` files that nodes memory-map.

Usage::

    python -m ddr.corpus build --docs 500000 --out data/corpus
"""

from __future__ import annotations

import argparse
import itertools
import os
from dataclasses import dataclass

import numpy as np

_ONSETS = ["b", "c", "d", "f", "g", "h", "j", "k", "l", "m", "n", "p", "r", "s", "t", "v", "w", "z",
           "br", "cr", "dr", "fl", "gr", "pl", "pr", "sh", "st", "tr", "th", "ch"]
_VOWELS = ["a", "e", "i", "o", "u", "ai", "ea", "io", "ou"]
_CODAS = ["", "n", "r", "s", "t", "l", "m", "x", "nd", "rk"]


def make_vocabulary(size: int, seed: int) -> list[str]:
    """Return ``size`` unique, pronounceable pseudo-words in a seeded order."""
    syllables = [o + v + c for o, v, c in itertools.product(_ONSETS, _VOWELS, _CODAS)]
    rng = np.random.default_rng(seed)
    words: list[str] = []
    seen: set[str] = set()
    n_syl = 2
    while len(words) < size:
        batch = rng.integers(0, len(syllables), size=(size * 2, n_syl))
        for row in batch:
            w = "".join(syllables[i] for i in row)
            if w not in seen:
                seen.add(w)
                words.append(w)
                if len(words) == size:
                    break
        n_syl += 1
    return words


@dataclass
class CorpusSpec:
    num_docs: int = 500_000
    vocab_size: int = 30_000
    num_topics: int = 400
    words_per_topic: int = 120
    topic_mix: float = 0.35          # fraction of tokens drawn from the doc's topic
    mean_len: float = 120.0
    seed: int = 7


@dataclass
class Corpus:
    """Flat token storage: doc ``i`` is ``tokens[offsets[i]:offsets[i+1]]``."""

    tokens: np.ndarray        # int32 term ids
    offsets: np.ndarray       # int64, len num_docs + 1
    doc_topic: np.ndarray     # int32
    topic_words: np.ndarray   # int32 [num_topics, words_per_topic]
    vocab: list[str]

    @property
    def num_docs(self) -> int:
        return len(self.offsets) - 1

    def doc_tokens(self, doc_id: int) -> np.ndarray:
        return self.tokens[self.offsets[doc_id]:self.offsets[doc_id + 1]]


def _zipf_probs(n: int, s: float = 1.0, q: float = 2.7) -> np.ndarray:
    ranks = np.arange(n, dtype=np.float64)
    w = 1.0 / np.power(ranks + q, s)
    return w / w.sum()


def generate(spec: CorpusSpec) -> Corpus:
    rng = np.random.default_rng(spec.seed)
    vocab = make_vocabulary(spec.vocab_size, spec.seed)

    # Topic words are drawn from the mid/low-frequency part of the vocabulary so
    # they are discriminative (high IDF), like real domain terminology.
    topic_words = np.stack([
        rng.choice(np.arange(300, spec.vocab_size), size=spec.words_per_topic, replace=False)
        for _ in range(spec.num_topics)
    ]).astype(np.int32)

    lengths = np.clip(
        rng.lognormal(mean=np.log(spec.mean_len), sigma=0.45, size=spec.num_docs), 20, 600
    ).astype(np.int64)
    offsets = np.zeros(spec.num_docs + 1, dtype=np.int64)
    np.cumsum(lengths, out=offsets[1:])
    total = int(offsets[-1])

    doc_topic = rng.integers(0, spec.num_topics, size=spec.num_docs).astype(np.int32)
    tok_topic = np.repeat(doc_topic, lengths)
    is_topic = rng.random(total) < spec.topic_mix

    tokens = rng.choice(spec.vocab_size, size=total, p=_zipf_probs(spec.vocab_size)).astype(np.int32)
    n_topic = int(is_topic.sum())
    local = rng.choice(spec.words_per_topic, size=n_topic, p=_zipf_probs(spec.words_per_topic, 0.9, 1.0))
    tokens[is_topic] = topic_words[tok_topic[is_topic], local]

    return Corpus(tokens=tokens, offsets=offsets, doc_topic=doc_topic,
                  topic_words=topic_words, vocab=vocab)


_ARRAYS = ("tokens", "offsets", "doc_topic", "topic_words")


def save(corpus: Corpus, path: str) -> None:
    """Write the corpus as a directory of ``.npy`` files (memory-mappable)."""
    tmp = path.rstrip("/") + ".tmp"
    os.makedirs(tmp, exist_ok=True)
    for name in _ARRAYS:
        np.save(os.path.join(tmp, name + ".npy"), getattr(corpus, name))
    with open(os.path.join(tmp, "vocab.txt"), "w") as f:
        f.write("\n".join(corpus.vocab))
    if os.path.exists(path):
        import shutil
        shutil.rmtree(path)
    os.replace(tmp, path)


def load(path: str) -> Corpus:
    """Memory-map the corpus so co-located nodes share one copy in the page cache."""
    arrs = {name: np.load(os.path.join(path, name + ".npy"), mmap_mode="r") for name in _ARRAYS}
    with open(os.path.join(path, "vocab.txt")) as f:
        vocab = f.read().split("\n")
    return Corpus(vocab=vocab, **arrs)


def load_or_build(path: str, spec: CorpusSpec | None = None) -> Corpus:
    if not os.path.exists(os.path.join(path, "vocab.txt")):
        save(generate(spec or CorpusSpec()), path)
    return load(path)


def make_queries(corpus: Corpus, n: int, seed: int = 11) -> list[str]:
    """Topical keyword queries: 2-3 topic terms, sometimes plus a common term."""
    rng = np.random.default_rng(seed)
    num_topics, wpt = corpus.topic_words.shape
    p_topic_word = _zipf_probs(wpt, 0.9, 1.0)
    queries = []
    for _ in range(n):
        t = rng.integers(num_topics)
        k = int(rng.integers(2, 4))
        words = [corpus.vocab[w] for w in rng.choice(corpus.topic_words[t], size=k, replace=False, p=p_topic_word)]
        if rng.random() < 0.5:  # common term -> long posting list, realistic cost
            words.append(corpus.vocab[int(rng.integers(0, 50))])
        queries.append(" ".join(words))
    return queries


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--docs", type=int, default=CorpusSpec.num_docs)
    b.add_argument("--vocab", type=int, default=CorpusSpec.vocab_size)
    b.add_argument("--seed", type=int, default=CorpusSpec.seed)
    b.add_argument("--out", default="data/corpus")
    args = ap.parse_args()
    if args.cmd == "build":
        c = generate(CorpusSpec(num_docs=args.docs, vocab_size=args.vocab, seed=args.seed))
        save(c, args.out)
        print(f"wrote {args.out}: {c.num_docs} docs, {len(c.tokens)} tokens, vocab {len(c.vocab)}")


if __name__ == "__main__":
    main()
