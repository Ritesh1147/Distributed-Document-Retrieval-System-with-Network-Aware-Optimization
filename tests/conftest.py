import os
import shutil
import socket
import subprocess
import time

import pytest

from ddr.corpus import CorpusSpec, generate, load, save

SMALL = CorpusSpec(num_docs=6000, vocab_size=4000, num_topics=60, words_per_topic=60, seed=3)


@pytest.fixture(scope="session")
def corpus_path(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("data") / "corpus")
    save(generate(SMALL), path)
    return path


@pytest.fixture(scope="session")
def corpus(corpus_path):
    return load(corpus_path)


@pytest.fixture(scope="session")
def redis_url():
    exe = shutil.which("redis-server")
    if not exe:
        pytest.skip("redis-server not installed")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    p = subprocess.Popen([exe, "--port", str(port), "--save", "", "--appendonly", "no"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
            break
        except OSError:
            time.sleep(0.05)
    yield f"redis://127.0.0.1:{port}/0"
    p.terminate()
    p.wait()
